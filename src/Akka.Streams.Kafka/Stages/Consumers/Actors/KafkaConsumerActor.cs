using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading;
using Akka.Actor;
using Akka.Event;
using Akka.Streams.Kafka.Extensions;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Internal;
using Akka.Streams.Kafka.Settings;
using Akka.Util;
using Akka.Util.Internal;
using Confluent.Kafka;
using Newtonsoft.Json;
using Decider = Akka.Streams.Supervision.Decider;
using Directive = Akka.Streams.Supervision.Directive;

namespace Akka.Streams.Kafka.Stages.Consumers.Actors
{
    /// <summary>
    /// Kafka consuming actor
    /// </summary>
    /// <typeparam name="K">Message key type</typeparam>
    /// <typeparam name="V">Message value type</typeparam>
    internal class KafkaConsumerActor<K, V> : ActorBase, ILogReceive
    {
        private readonly IActorRef _owner;
        private ConsumerSettings<K, V> _settings;
        /// <summary>
        /// Stores delegates for external handling of statistics
        /// </summary>
        private readonly IStatisticsHandler _statisticsHandler;

        /// <summary>
        /// Stores delegates for external handling of partition events
        /// </summary>
        private readonly IPartitionEventHandler _partitionEventHandler;
        
        private readonly RestrictedConsumer<K, V> _restrictedConsumer;
        private readonly TimeSpan _warningDuration;
        
        private ICancelable _pollCancellation;
        // private readonly Internal.Poll<K, V> _pollMessage;
        // private readonly Internal.Poll<K, V> _delayedPollMessage;

        private TimeSpan _pollTimeout;
        
        /// <summary>
        /// Limits the blocking on offsetForTimes
        /// </summary>
        private TimeSpan _offsetForTimesTimeout;

        private ImmutableDictionary<TopicPartition, TopicPartitionOffset> _seekedOffset = ImmutableDictionary<TopicPartition, TopicPartitionOffset>.Empty;

        /// <summary>
        /// Limits the blocking on position in [[RebalanceListenerImpl]]
        /// </summary>
        private TimeSpan _positionTimeout;

        /// <summary>
        /// Stores all incoming requests from consuming kafka stages
        /// </summary>
        private IImmutableDictionary<IActorRef, KafkaConsumerActorMetadata.Internal.RequestMessages> _requests 
            = ImmutableDictionary<IActorRef, KafkaConsumerActorMetadata.Internal.RequestMessages>.Empty;
        /// <summary>
        /// Stores stage actors, requesting for more messages
        /// </summary>
        private IImmutableSet<IActorRef> _requestors = ImmutableHashSet<IActorRef>.Empty;
        private ICommitRefreshing<K, V> _commitRefreshing;
        private IConsumer<K, V> _consumer;
        private IActorRef _connectionCheckerActor;
        private readonly ILoggingAdapter _log;
        private bool _stopInProgress = false;
        private bool _delayedPollInFlight = false;
        private IImmutableSet<TopicPartition> _resumedPartitions = ImmutableHashSet<TopicPartition>.Empty;
        private readonly Decider _decider;

        /// <summary>
        /// While `true`, committing is delayed.
        /// Changed by `onPartitionsRevoked` and `onPartitionsAssigned` callbacks
        /// </summary>
        private bool _rebalanceInProgress = false;
        /// <summary>
        /// Keeps commit offsets during rebalances for later commit.
        /// </summary>
        private IImmutableSet<TopicPartitionOffset> _rebalanceCommitStash = ImmutableHashSet<TopicPartitionOffset>.Empty;
        /// <summary>
        /// Keeps commit senders that need a reply once stashed commits are made.
        /// </summary>
        private IImmutableList<IActorRef> _rebalanceCommitSenders = ImmutableArray<IActorRef>.Empty;

        /// <summary>
        /// KafkaConsumerActor
        /// </summary>
        /// <param name="owner">Owner actor to send critical failures to</param>
        /// <param name="settings">Consumer settings</param>
        /// <param name="statisticsHandler">Statistics handler</param>
        /// <param name="decider"></param>
        /// <param name="partitionEventHandler">Partion events handler</param>
        public KafkaConsumerActor(IActorRef owner, ConsumerSettings<K, V> settings, Decider decider, IPartitionEventHandler partitionEventHandler, IStatisticsHandler statisticsHandler)
        {
            _owner = owner;
            _settings = settings;
            _decider = decider;
            _statisticsHandler = statisticsHandler;
            _partitionEventHandler = partitionEventHandler;
            
            var restrictedConsumerTimeoutMs = Math.Round(_settings.PartitionHandlerWarning.TotalMilliseconds * 0.95);
            _restrictedConsumer = new RestrictedConsumer<K, V>(_consumer, TimeSpan.FromMilliseconds(restrictedConsumerTimeoutMs));
            _warningDuration = _settings.PartitionHandlerWarning;
            
            // _pollMessage = new Internal.Poll<K, V>(this, periodic: true);
            // _delayedPollMessage = new Internal.Poll<K, V>(this, periodic: false);
            _log = Context.GetLogger();
        }

        #region Rebalance listener
        
        internal sealed class PartitionAssigned
        {
            public PartitionAssigned(IImmutableSet<TopicPartition> partitions)
            {
                Partitions = partitions;
            }

            public IImmutableSet<TopicPartition> Partitions { get; }
        }
        
        internal sealed class PartitionRevoked
        {
            public PartitionRevoked(IImmutableSet<TopicPartitionOffset> partitions)
            {
                Partitions = partitions;
            }

            public IImmutableSet<TopicPartitionOffset> Partitions { get; }
        }
    
        // This is RebalanceListener.OnPartitionAssigned on JVM
        private void PartitionsAssignedHandler(IImmutableSet<TopicPartition> partitions)
        {
            var correlationId = Guid.NewGuid();
            
            _log.Info("[{CorrelationId}] Partitions assigned: {Partitions}", correlationId, partitions.Select(tp => tp.ToString()).JoinToString(", "));
            
            var assignment = _consumer.Assignment;
            var partitionsToPause = partitions.Where(p => assignment.Contains(p)).ToList();
            PausePartitions(correlationId, partitionsToPause);
            
            _commitRefreshing.AssignedPositions(partitions, _consumer, _settings.PositionTimeout);

            var watch = Stopwatch.StartNew();
            _partitionEventHandler.OnAssign(partitions, _restrictedConsumer);
            watch.Stop();
            CheckDuration(watch, "onAssign");
            
            _rebalanceInProgress = false;
        }

        // This is RebalanceListener.OnPartitionRevoked on JVM
        private void PartitionsRevokedHandler(IImmutableSet<TopicPartitionOffset> partitions)
        {
            var correlationId = Guid.NewGuid();
            
            _log.Info("[{CorrelationId}] Partitions revoked: {Partitions}", correlationId, partitions.Select(p => p.TopicPartition.ToString()).JoinToString(", "));
            
            var watch = Stopwatch.StartNew();
            _partitionEventHandler.OnRevoke(partitions, _restrictedConsumer);
            watch.Stop();
            CheckDuration(watch, "onRevoke");
            
            _commitRefreshing.Revoke(partitions.Select(tp => tp.TopicPartition).ToImmutableHashSet());
            
            // TODO: Remove requests and requestors for TopicPartitions that have been revoked
            // _requests = ImmutableDictionary<IActorRef, KafkaConsumerActorMetadata.Internal.RequestMessages>.Empty;
            // _requestors = ImmutableHashSet<IActorRef>.Empty;
            
            _rebalanceInProgress = true;
        }

        private void RebalancePostStop()
        {
            var currentTopicPartitions = _consumer.Assignment;
            PausePartitions(Guid.NewGuid(), currentTopicPartitions);
            
            var watch = Stopwatch.StartNew();
            _partitionEventHandler.OnStop(currentTopicPartitions.ToImmutableHashSet(), _restrictedConsumer);
            watch.Stop();
            CheckDuration(watch, "onStop");
        }

        private void CheckDuration(Stopwatch watch, string method)
        {
            if (watch.Elapsed > _warningDuration)
            {
                _log.Warning("Partition assignment handler `{Method}` took longer than `partition-handler-warning`: {Elapsed} ms", method, watch.ElapsedMilliseconds);
            }
        }        

        #endregion
        
        protected override bool Receive(object message)
        {
            switch (message)
            {
                case KafkaConsumerActorMetadata.Internal.Assign assign:
                {
                    ScheduleFirstPollTask();
                    CheckOverlappingRequests("Assign", Sender, assign.TopicPartitions);
                    var previousAssigned = _consumer.Assignment;
                    _consumer.Assign(assign.TopicPartitions.Union(previousAssigned));
                    _commitRefreshing.AssignedPositions(assign.TopicPartitions, _consumer, _settings.PositionTimeout);
                    return true;
                }

                case KafkaConsumerActorMetadata.Internal.AssignWithOffset assignWithOffset:
                {
                    ScheduleFirstPollTask();
                    var topicPartitions = assignWithOffset.TopicPartitionOffsets.Select(o => o.TopicPartition).ToImmutableHashSet();
                    CheckOverlappingRequests("AssignWithOffset", Sender, topicPartitions);
                    var previousAssigned = _consumer.Assignment.Select(tp => new TopicPartitionOffset(tp, new Offset(0)));
                    _consumer.Assign(assignWithOffset.TopicPartitionOffsets.Union(previousAssigned));
                    _commitRefreshing.AssignedPositions(topicPartitions, assignWithOffset.TopicPartitionOffsets);
                    return true;
                }
                    
                case KafkaConsumerActorMetadata.Internal.Commit commit when _rebalanceInProgress:
                    _rebalanceCommitStash = _rebalanceCommitStash.Union(commit.Offsets);
                    _rebalanceCommitSenders = _rebalanceCommitSenders.Add(Sender);
                    return true;
                
                case KafkaConsumerActorMetadata.Internal.Commit commit:
                    _commitRefreshing.Add(commit.Offsets);
                    var replyTo = Sender;
                    Commit(commit.Offsets, msg => replyTo.Tell(msg));
                    return true;
                
                case Internal.Poll<K, V> poll:
                    
                    _log.Info("[{CorrelationId}] Poll requested, periodic: {periodic}", poll.CorrelationId, poll.Periodic);
                    
                    ReceivePoll(poll);
                    return true;
                
                case KafkaConsumerActorMetadata.Internal.ISubscriptionRequest subscribe:
                    HandleSubscription(subscribe);
                    return true;
                
                case KafkaConsumerActorMetadata.Internal.RequestMessages requestMessages:
                    var correlationId = Guid.NewGuid();
                    
                    _log.Info("[{CorrelationId}] Messages requested from: {Sender}, for: {TopicPartitions}", correlationId, Sender, requestMessages.Topics.JoinToString(", "));
                    
                    Context.Watch(Sender);
                    CheckOverlappingRequests("RequestMessages", Sender, requestMessages.Topics);
                    _requests = _requests.SetItem(Sender, requestMessages);
                    _requestors = _requestors.Add(Sender);
                    
                    // When many requestors, e.g. many partitions with committablePartitionedSource the
                    // performance is much by collecting more requests/commits before performing the poll.
                    // That is done by sending a message to self, and thereby collect pending messages in mailbox.
                    if (_requestors.Count == 1)
                    {
                        _log.Info("[{CorrelationId}] Polling for single requestor when messages requested", correlationId);
                        Poll(correlationId);
                    }
                    else if (!_delayedPollInFlight)
                    {
                        _delayedPollInFlight = true;
                        // Self.Tell(_delayedPollMessage);
                        var delayedPoll = new Internal.Poll<K, V>(this, periodic: false, correlationId: correlationId);
                        _log.Info("[{CorrelationId}] Delayed poll when messages requested, periodic: {Periodic}", delayedPoll.CorrelationId, delayedPoll.Periodic);
                        Self.Tell(delayedPoll);
                    }
                    return true;
                
                case KafkaConsumerActorMetadata.Internal.Seek seek:
                    foreach (var offset in seek.Offsets)
                    {
                        _seekedOffset = _seekedOffset.SetItem(offset.TopicPartition, offset);
                    }
                    Sender.Tell(Done.Instance);
                    return true;
                    
                
                case KafkaConsumerActorMetadata.Internal.Committed committed:
                    _commitRefreshing.Committed(committed.Offsets);
                    return true;
                
                case KafkaConsumerActorMetadata.Internal.Stop _:
                    _log.Debug("Received Stop from {Sender}, stopping", Sender);
                    Context.Stop(Self);
                    return true;
                
                case KafkaConnectionFailed kcf:
                    ProcessError(Guid.NewGuid(), kcf);
                    Self.Tell(KafkaConsumerActorMetadata.Internal.Stop.Instance);
                    return true;

                case Terminated terminated:
                    _log.Debug("Terminated requestor: {TerminatedActorRef}", terminated.ActorRef);
                    _requests = _requests.Remove(terminated.ActorRef);
                    _requestors = _requestors.Remove(terminated.ActorRef);
                    return true;

                case Metadata.IRequest req:
                    Sender.Tell(HandleMetadataRequest(req));
                    return true;
                
                // Rebalance callbacks
                case PartitionAssigned evt:
                    PartitionsAssignedHandler(evt.Partitions);
                    return true;
                
                case PartitionRevoked evt:
                    PartitionsRevokedHandler(evt.Partitions);
                    return true;
                
                case Status.Failure fail:
                    ProcessExceptions(Guid.NewGuid(), fail.Cause);
                    return true;
                
                default:
                    return false;
            }
        }
       
        protected override void PreStart()
        {
            base.PreStart();

            try
            {
                ApplySettings(_settings);
            }
            catch (Exception ex)
            {
                _owner?.Tell(new Status.Failure(ex));
                throw;
            }
        }

        private void ApplySettings(ConsumerSettings<K, V> updatedSettings)
        {
            _settings = updatedSettings;
            _pollTimeout = _settings.PollTimeout;
            _positionTimeout = _settings.PositionTimeout;
            _commitRefreshing = CommitRefreshing.Create<K, V>(_settings.CommitRefreshInterval);
            try
            {
                if (_log.IsDebugEnabled)
                    _log.Debug($"Creating Kafka consumer with settings: {JsonConvert.SerializeObject(_settings)}");

                var localSelf = Self;
                _consumer = _settings.CreateKafkaConsumer(
                    consumeErrorHandler: (_, e) => localSelf.Tell(new Status.Failure(new KafkaException(e))),
                    partitionAssignedHandler: (_, tp) => localSelf.Tell(new PartitionAssigned(tp.ToImmutableHashSet())),
                    partitionRevokedHandler: (_, tp) => localSelf.Tell(new PartitionRevoked(tp.ToImmutableHashSet())),
                    statisticHandler: (c, json) => _statisticsHandler.OnStatistics(c, json));

                if (_settings.ConnectionCheckerSettings.Enabled)
                {
                    _connectionCheckerActor = Context.ActorOf(ConnectionChecker.Props(_settings.ConnectionCheckerSettings));
                }
            }
            catch (Exception e)
            {
                ProcessError(Guid.NewGuid(), e);
                throw;
            }
        }

        protected override void PostStop()
        {
            base.PostStop();
            try
            {
                _pollCancellation?.Cancel(); // Stop existing scheduling, if any
                
                if (_settings.ConnectionCheckerSettings.Enabled)
                {
                    _connectionCheckerActor.Tell(KafkaConsumerActorMetadata.Internal.Stop.Instance);
                }

                // reply to outstanding requests is important if the actor is restarted
                foreach (var (actorRef, request) in _requests.ToTuples())
                {
                    var emptyMessages = new KafkaConsumerActorMetadata.Internal.Messages<K, V>(request.RequestId,
                        ImmutableList<ConsumeResult<K, V>>.Empty);
                    actorRef.Tell(emptyMessages);
                }

                RebalancePostStop();
            }
            finally
            {
                // Make sure that the consumer is unassigned from the partition AND closed before we dispose
                try { _consumer.Unassign(); }
                catch (Exception) { /* no-op */ }

                try { _consumer.Close(); }
                catch (Exception) { /* no-op */ }

                _consumer.Dispose();
            }
        }

        private void HandleSubscription(KafkaConsumerActorMetadata.Internal.ISubscriptionRequest subscriptionRequest)
        {
            try
            {
                if (subscriptionRequest is KafkaConsumerActorMetadata.Internal.Subscribe subscribe)
                    _consumer.Subscribe(subscribe.Topics);
                else if (subscriptionRequest is KafkaConsumerActorMetadata.Internal.SubscribePattern subscribePattern)
                    _consumer.Subscribe(subscribePattern.TopicPattern);
                else
                    throw new NotSupportedException($"Unsupported subscription type: {subscriptionRequest.GetType()}");
                
                ScheduleFirstPollTask();
            }
            catch (Exception ex)
            {
                ProcessError(Guid.NewGuid(), ex);
            }
        }

        private Metadata.IResponse HandleMetadataRequest(Metadata.IRequest req)
        {
            switch (req)
            {
                case Metadata.ListTopics _:
                    return new Metadata.Topics(Try<List<TopicMetadata>>
                        .From(() =>
                        {
                            using (var adminClient = new DependentAdminClientBuilder(_consumer.Handle).Build())
                            {
                                return adminClient.GetMetadata(_settings.MetadataRequestTimeout).Topics;
                            }
                        }));
                default:
                    throw new InvalidOperationException($"Unknown metadata request: {req}");
            }
        }

        private void ScheduleFirstPollTask()
        {
            if (_pollCancellation == null || _pollCancellation.IsCancellationRequested)
            {
                _log.Debug("Scheduling first poll task...");
                SchedulePollTask();
            }
        }

        private void SchedulePollTask()
        {
            _pollCancellation?.Cancel(); // Stop existing scheduling, if any

            // var pm = _pollMessage;
            var poll = new Internal.Poll<K, V>(this, periodic: true, correlationId: Guid.NewGuid());
            
            _log.Debug("[{CorrelationId}] Scheduling poll, periodic: {Periodic}, delay: {Delay}ms...", poll.CorrelationId, poll.Periodic, _settings.PollInterval.TotalMilliseconds);
            _pollCancellation = Context.System.Scheduler.ScheduleTellOnceCancelable(_settings.PollInterval, Self, poll, Self);
        }

        private void CheckOverlappingRequests(string updateType, IActorRef fromStage, IImmutableSet<TopicPartition> topics)
        {
            // check if same topics/partitions have already been requested by someone else,
            // which is an indication that something is wrong, but it might be alright when assignments change.
            foreach (var (actorRef, request) in _requests.ToTuples())
            {
                if (!actorRef.Equals(fromStage) && request.Topics.Any(topics.Contains))
                {
                    _log.Warning($"{updateType} from topic/partition {string.Join(", ", topics)} " +
                                 $"already requested by other stage {string.Join(", ", request.Topics)}");
                    actorRef.Tell(new KafkaConsumerActorMetadata.Internal.Messages<K, V>(request.RequestId, ImmutableList<ConsumeResult<K, V>>.Empty));
                    _requests = _requests.Remove(actorRef);
                }
            }
        }

        private void ReceivePoll(Internal.Poll<K, V> poll)
        {
            if (poll.Target == this)
            {
                var refreshOffsets = _commitRefreshing.RefreshOffsets;
                if (refreshOffsets.Any())
                {
                    _log.Debug("[{CorrelationId}] Refreshing committed offsets: {Offsets}", poll.CorrelationId, refreshOffsets.JoinToString(", "));
                    Commit(refreshOffsets, msg => Context.System.DeadLetters.Tell(msg));
                }
               
                Poll(poll.CorrelationId);
               
                if (poll.Periodic)
                    SchedulePollTask();
                else
                    _delayedPollInFlight = false;
            }
            else
            {
                // Message was enqueued before a restart - can be ignored
                _log.Debug("[{CorrelationId}] Ignoring Poll message with stale target ref", poll.CorrelationId);
            }
        }

        private void Poll(Guid pollCorrelationId)
        {
            var currentAssignment = _consumer.Assignment;
            var initialRebalanceInProcess = _rebalanceInProgress;

            // TODO: handle no assignments

            // if (currentAssignment.IsEmpty())
            // {
            //     _log.Info("[{CorrelationId}] Assignment is empty - skipping poll, outstanding {RequestCount} requests", pollCorrelationId, _requests.Count);
            //     
            //     try
            //     {
            //         var consumed = _consumer.Consume(0);
            //         if (consumed != null)
            //             throw new IllegalActorStateException("Consumed message should be null");
            //     }
            //     catch (Exception e)
            //     {
            //         ProcessExceptions(pollCorrelationId, e);
            //     }
            //     
            //     return;
            // }
            
            if (_requests.IsEmpty())
            {
                if(_log.IsDebugEnabled)
                    _log.Debug("[{CorrelationId}] Requests are empty - attempting to consume", pollCorrelationId);
                PausePartitions(pollCorrelationId, currentAssignment);
                try
                {
                    var consumed = _consumer.Consume(0);
                    if (consumed != null)
                        throw new IllegalActorStateException("Consumed message should be null");
                }
                catch (Exception e)
                {
                    ProcessExceptions(pollCorrelationId, e);
                }
            }
            else
            {
                // Seek has to be done here because they can somehow fail.
                // Would need to see if we can move this somewhere else
                // because a seek can take up to 200ms to complete
                foreach (var tpo in _seekedOffset.Select(kvp => kvp.Value))
                {
                    try
                    {
                        if(_log.IsDebugEnabled)
                            _log.Debug("[{CorrelationId}] Seeking offset {Topic}[{Partition}][{Offset}]", pollCorrelationId, tpo.Topic, tpo.Partition, tpo.Offset);
                        _consumer.Seek(tpo);
                    }
                    catch (Exception ex)
                    {
                        _log.Error(ex, "{TopicPartition} Failed to seek to {Offset}: {Msg}", tpo.TopicPartition, tpo.Offset, ex.Message);
                        throw;
                    }
                }
                
                _log.Info("[{CorrelationId}] Starting poll with rebalancing: {Rebalancing}, {RequestCount} requests: {Requests}, {AssignmentCount} assignments: {Assignments}",
                    pollCorrelationId, _rebalanceInProgress, _requests.Count, _requests.SelectMany(r => r.Value.Topics).Select(tp => tp.ToString()).JoinToString(", "),
                    _consumer.Assignment.Count, _consumer.Assignment.Select(tp => tp.ToString()).JoinToString(", "));
                
                // resume partitions to fetch
                IImmutableSet<TopicPartition> partitionsToFetch = _requests.Values.SelectMany(v => v.Topics).ToImmutableHashSet();
                var resumeThese = currentAssignment.Where(partitionsToFetch.Contains).ToList();
                var pauseThese = currentAssignment.Except(resumeThese).ToList();
                PausePartitions(pollCorrelationId, pauseThese);
                ResumePartitions(pollCorrelationId, resumeThese);

                using (var cts = new CancellationTokenSource(_settings.PollTimeout))
                {
                    var (polled, exception) = PollKafka(cts.Token);
                    try
                    {
                        _log.Info("[{CorrelationId}] Processing {polled.Count} records, {AssignmentCount} assignments: {Assignments}",
                            pollCorrelationId, polled.Count, _consumer.Assignment.Count, _consumer.Assignment.Select(tp => tp.ToString()).JoinToString(", "));
                        
                        ProcessResult(pollCorrelationId, partitionsToFetch, polled);
                    }
                    catch (Exception e)
                    {
                        ProcessExceptions(pollCorrelationId, e);
                    }

                    ProcessExceptions(pollCorrelationId, exception);
                }
            }
            
            CheckRebalanceState(initialRebalanceInProcess);

            if (_stopInProgress)
            {
                _log.Debug("Stopping");
                Context.Stop(Self);
            }
        }

        private void ProcessExceptions(Guid pollCorrelationId, Exception exception)
        {
            if (exception == null)
                return;

            var directive = _decider(exception);
            ProcessError(pollCorrelationId, exception);
            if (directive == Directive.Resume)
                return;
            
             _pollCancellation?.Cancel();
            if(directive == Directive.Stop && _log.IsErrorEnabled)
                _log.Error(exception, "[{CorrelationId}] Exception when polling from consumer, KafkaConsumerActor actor: {0}", pollCorrelationId, exception.Message);
            Context.Stop(Self);
        }

        private (List<ConsumeResult<K, V>>, Exception) PollKafka(CancellationToken token)
        {
            ConsumeResult<K, V> consumed;
            var i = 10; // 10 poll attempts
            var timeout = Math.Max((int) _pollTimeout.TotalMilliseconds / i, 1);
            var polled = new List<ConsumeResult<K, V>>();
            do
            {
                try
                {
                    // this would return immediately if there are messages waiting inside the client queue buffer
                    consumed = _consumer.Consume(timeout);
                }
                catch (Exception e)
                {
                    return (polled, e);
                }
                if (consumed != null)
                    polled.Add(consumed);
                i--;
            } while (i > 0 && consumed != null && !token.IsCancellationRequested);

            return (polled, null);
        }

        private void ProcessResult(Guid pollCorrelationId, IImmutableSet<TopicPartition> partitionsToFetch, List<ConsumeResult<K,V>> rawResult)
        {
            if(_log.IsDebugEnabled)
                _log.Debug("[{CorrelationId}] Processing poll result with {RecordCount} records", pollCorrelationId, rawResult.Count);
            if(rawResult.IsEmpty())
                return;

            var fetchedTps = rawResult.Select(m => m.TopicPartition).ToImmutableSet();
            if (!fetchedTps.Except(partitionsToFetch).IsEmpty())
                throw new ArgumentException(
                    $"Unexpected records polled. Expected: [{string.Join(", ", partitionsToFetch.Select(p => p.ToString()))}], " +
                    $"result: [{string.Join(", ", fetchedTps.Select(p => p.ToString()))}], " +
                    $"consumer assignment: [{_consumer.Assignment.Select(tp => tp.ToString()).JoinToString(", ")}]");
                    
            //send messages to actors
            foreach (var (stageActorRef, request) in _requests.ToTuples())
            {
                var messages = new List<ConsumeResult<K, V>>();
                foreach (var message in rawResult)
                {
                    var currentTp = message.TopicPartition;
                    
                    if (_seekedOffset.TryGetValue(currentTp, out var seekedTpo))
                    {
                        if (message.Offset != seekedTpo.Offset)
                            throw new Exception("Seek failed, received message offset is greater than seek offset");
                        _seekedOffset = _seekedOffset.Remove(currentTp);
                    }
                    
                    // If requestor is interested in consumed topic, send him consumed result
                    if (request.Topics.Contains(currentTp))
                    {
                        messages.Add(message);
                    }
                }
                if(!messages.IsEmpty())
                {
                    _log.Info("[{CorrelationId}] Sending {MessageCount} messages to: {StageActorRef} for: {TopicPartition}", pollCorrelationId, messages.Count, stageActorRef, messages.First().TopicPartition);
                    
                    stageActorRef.Tell(new KafkaConsumerActorMetadata.Internal.Messages<K, V>(request.RequestId, messages.ToImmutableList()));
                    _requests = _requests.Remove(stageActorRef);
                }
            }                    
        }
        
        private void ProcessError(Guid pollCorrelationId, Exception error)
        {
            var involvedStageActors = _requests.Keys.Append(_owner).ToImmutableHashSet();
            _log.Info("[{CorrelationId}] Sending failure to {InvolvedStageActors}. Error: {Error}", pollCorrelationId, involvedStageActors.JoinToString(", "), error.Message);
            foreach (var actor in involvedStageActors)
            {
                actor.Tell(new Status.Failure(error));
            }
        }

        private void Commit(IImmutableSet<TopicPartitionOffset> commitMap, Action<object> sendReply)
        {
            try
            {
                _commitRefreshing.UpdateRefreshDeadlines(commitMap.Select(tp => tp.TopicPartition).ToImmutableHashSet());

                var watch = Stopwatch.StartNew();
                
                _consumer.Commit(commitMap);
                
                watch.Stop();
                if (watch.Elapsed >= _settings.CommitTimeWarning)
                    _log.Warning($"Kafka commit took longer than `commit-time-warning`: {watch.ElapsedMilliseconds} ms");

                Self.Tell(new KafkaConsumerActorMetadata.Internal.Committed(commitMap));
                sendReply(Done.Instance);
            }
            catch (Exception ex)
            {
                sendReply(new Status.Failure(ex));
            }

            // When many requestors, e.g. many partitions with committablePartitionedSource the
            // performance is much by collecting more requests/commits before performing the poll.
            // That is done by sending a message to self, and thereby collect pending messages in mailbox.
            if (_requestors.Count == 1)
            {
                var correlationId = Guid.NewGuid();
                _log.Debug("[{CorrelationId}] Polling for single requestor after commit...", correlationId);
                Poll(Guid.NewGuid());
            }
            else if (!_delayedPollInFlight)
            {
                _delayedPollInFlight = true;
                // Self.Tell(_delayedPollMessage);
                var delayedPoll = new Internal.Poll<K, V>(this, periodic: false, correlationId: Guid.NewGuid());
                _log.Debug("[{CorrelationId}] Delayed poll after commit, periodic: {Periodic}", delayedPoll.CorrelationId, delayedPoll.Periodic);
                Self.Tell(delayedPoll);
            }
        }

        /// <summary>
        /// Detects state changes of <see cref="_rebalanceInProgress"/> and takes action on it.
        /// </summary>
        private void CheckRebalanceState(bool initialRebalanceInProgress)
        {
            if (initialRebalanceInProgress && !_rebalanceInProgress && _rebalanceCommitSenders.Any())
            {
                _log.Debug($"Comitting stash {string.Join(", ", _rebalanceCommitStash)} replying to {string.Join(", ", _rebalanceCommitSenders)}");
                var replyTo = _rebalanceCommitSenders;
                Commit(_rebalanceCommitStash, msg => replyTo.ForEach(actor => actor.Tell(msg)));
                _rebalanceCommitStash = ImmutableHashSet<TopicPartitionOffset>.Empty;
                _rebalanceCommitSenders = ImmutableList<IActorRef>.Empty;
            }
        }

        private void PausePartitions(Guid pollCorrelationId, List<TopicPartition> partitions)
        {
            // if(_log.IsDebugEnabled)
            if(!partitions.IsEmpty())
                _log.Info("[{CorrelationId}] Pausing partitions [{Partitions}]", pollCorrelationId, partitions.JoinToString(", "));
            _consumer.Pause(partitions);
            _resumedPartitions = _resumedPartitions.Except(partitions);
        }

        private void ResumePartitions(Guid pollCorrelationId, List<TopicPartition> partitions)
        {
            var partitionsToResume = partitions.Except(_resumedPartitions).ToList();
            // if(_log.IsDebugEnabled)
            if(!partitionsToResume.IsEmpty())
                _log.Info("[{CorrelationId}] Resuming partitions [{Partitions}]", pollCorrelationId, partitionsToResume.JoinToString(", "));
            _consumer.Resume(partitionsToResume);
            _resumedPartitions = _resumedPartitions.Union(partitionsToResume);
        }

        static class Internal
        {
            public class Poll<TPollKey, TPollValue> 
                where TPollKey : K
                where TPollValue : V
            {
                public Poll(KafkaConsumerActor<TPollKey, TPollValue> target, bool periodic, Guid correlationId)
                {
                    Target = target;
                    Periodic = periodic;
                    CorrelationId = correlationId;
                }

                public KafkaConsumerActor<TPollKey, TPollValue> Target { get; }
                public bool Periodic { get; }
                public Guid CorrelationId { get; }
            }
        }

    }
}