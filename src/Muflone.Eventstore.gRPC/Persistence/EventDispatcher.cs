using EventStore.Client;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Muflone.Messages.Events;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections.Concurrent;
using System.Text;

namespace Muflone.Eventstore.gRPC.Persistence
{
    public class EventDispatcher : IHostedService
    {
        private const int ReconnectTimeoutMillisec = 5000;
        private const int ThreadKillTimeoutMillisec = 5000;
        private const int ReadPageSize = 500;
        private const int LiveQueueSizeLimit = 10000;

        private readonly IEventBus eventBus;
        private readonly EventStoreClient eventStoreClient;
        private readonly IEventStorePositionRepository eventStorePositionRepository;
        private readonly ManualResetEventSlim historicalDone = new(true);
        private readonly ConcurrentQueue<ResolvedEvent> historicalQueue = new();
        private readonly ManualResetEventSlim liveDone = new(true);
        private readonly ConcurrentQueue<ResolvedEvent> liveQueue = new();
        private readonly ILogger log;
        //private EventStoreSubscription eventStoreSubscription = null!;
        private int isPublishing;
        private Position lastProcessed;
        private volatile bool livePublishingAllowed;

        private volatile bool stop;

        public EventDispatcher(ILoggerFactory loggerFactory, EventStoreClient store, IEventBus eventBus,
            IEventStorePositionRepository eventStorePositionRepository)
        {
            log = loggerFactory?.CreateLogger(GetType()) ?? throw new ArgumentNullException(nameof(loggerFactory));
            eventStoreClient = store ?? throw new ArgumentNullException(nameof(store));
            this.eventBus = eventBus ?? throw new ArgumentNullException(nameof(eventBus));
            this.eventStorePositionRepository = eventStorePositionRepository;
            var position = eventStorePositionRepository.GetLastPosition().GetAwaiter().GetResult();
            lastProcessed = new Position(position.CommitPosition, position.PreparePosition);
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            return StartDispatching();
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return StopDispatching();
        }

        // Credit algorithm to Szymon Pobiega
        // http://simon-says-architecture.com/2013/02/02/mechanics-of-durable-subscription/#comments
        // 1. The subscriber always starts with pull assuming there were some messages generated while it was offline
        // 2. The subscriber pulls messages until there's nothing left to pull (it is up to date with the stream)
        // 3. Push subscription is started but arriving messages are not processed immediately but temporarily redirected to a buffer
        // 4. One last pull is done to ensure nothing happened between step 2 and 3
        // 5. Messages from this last pull are processed
        // 6. Processing messages from push buffer is started. While messages are processed, they are checked against IDs of messages processed in step 5 to ensure there's no duplicates.
        // 7. System works in push model until subscriber is killed or subscription is dropped by publisher drops push subscription.

        //Credit to Andrii Nakryiko
        //If data is written to storage at such a speed, that between the moment you did your last 
        //pull read and the moment you subscribed to push notifications more data (events) were 
        //generated, than you request in one pull request, you would need to repeat steps 4-5 few 
        //times until you get a pull message which position is >= subscription position 
        //(EventStore provides you with those positions).
        public Task StartDispatching()
        {
            return RecoverSubscription();
        }

        private async Task RecoverSubscription()
        {
            livePublishingAllowed = false;
            liveDone.Wait(); // wait until all live processing is finished (queue is empty, _lastProcessed updated)

            var nextPos = await ReadHistoricalEventsFrom(lastProcessed);

            eventStoreSubscription = await SubscribeToAll();

            await ReadHistoricalEventsFrom(nextPos);
            historicalDone.Wait(); // wait until historical queue is empty and _lastProcessed updated

            livePublishingAllowed = true;
            EnsurePublishEvents(liveQueue, liveDone);
        }

        public Task StopDispatching()
        {
            stop = true;
            eventStoreSubscription?.Unsubscribe();

            // hopefully additional check in PublishEvents (additional check for _stop after setting event) prevents race conditions
            if (!historicalDone.Wait(ThreadKillTimeoutMillisec))
                throw new TimeoutException("EventDispatchStoppingException");

            if (!liveDone.Wait(ThreadKillTimeoutMillisec))
                throw new TimeoutException("EventDispatchStoppingException");
            return Task.CompletedTask;
        }

        private async Task<Position> ReadHistoricalEventsFrom(Position from, CancellationToken cancellationToken = default)
        {
            Position? position = from;
            while (!stop)
            {
                var readResult = eventStoreClient.ReadAllAsync(Direction.Forwards, position.HasValue ? position.Value : Position.Start, cancellationToken: cancellationToken);
                if (await readResult.CountAsync(cancellationToken) == 0)
                    break;
                await readResult.ForEachAsync(@event => historicalQueue.Enqueue(@event), cancellationToken);
                //await foreach (var rawEvent in readResult) 
                //    historicalQueue.Enqueue(rawEvent);
                EnsurePublishEvents(historicalQueue, historicalDone);

                position = readResult.LastPosition;
            }

            return position ?? Position.Start;
        }

        private Task<EventStoreSubscription> SubscribeToAll()
        {
            //TODO: Before trying to resubscribe - how to ensure that store is active and ready to accept.
            //AN: eventStoreClient automatically tries to connect (if not already connected) to EventStore, so you don't have to do something manually
            //Though in case of errors, you need to do some actions (if EventStore server is down or not yet up, etc)
            var task = eventStoreClient.SubscribeToAllAsync(false, EventAppeared, SubscriptionDropped);
            if (!task.Wait(ReconnectTimeoutMillisec))
                throw new TimeoutException("ReconnectedAfterSubscriptionException");
            return Task.FromResult(task.GetAwaiter().GetResult());

            //return await eventStoreClient.SubscribeToAllAsync(false, EventAppeared, SubscriptionDropped);
        }

        private void SubscriptionDropped(EventStoreSubscription subscription, SubscriptionDropReason dropReason, Exception exception)
        {
            if (stop)
                return;

            RecoverSubscription().GetAwaiter().GetResult();
        }

        private Task EventAppeared(EventStoreSubscription eventStoreSubscription, ResolvedEvent resolvedEvent)
        {
            if (stop)
                return Task.CompletedTask;

            liveQueue.Enqueue(resolvedEvent);

            //Prevent live queue memory explosion.
            if (!livePublishingAllowed && liveQueue.Count > LiveQueueSizeLimit) liveQueue.TryDequeue(out var throwAwayEvent);

            if (livePublishingAllowed)
                EnsurePublishEvents(liveQueue, liveDone);
            return Task.CompletedTask;
        }

        private void EnsurePublishEvents(ConcurrentQueue<ResolvedEvent> queue, ManualResetEventSlim doneEvent)
        {
            if (stop) return;

            if (Interlocked.CompareExchange(ref isPublishing, 1, 0) == 0)
                ThreadPool.QueueUserWorkItem(_ => PublishEvents(queue, doneEvent));
        }

        private void PublishEvents(ConcurrentQueue<ResolvedEvent> queue, ManualResetEventSlim doneEvent)
        {
            var keepGoing = true;
            while (keepGoing)
            {
                doneEvent.Reset(); // signal we start processing this queue
                if (stop) // this is to avoid race condition in StopDispatching, though it is 1AM here, so I could be wrong :)
                {
                    doneEvent.Set();
                    Interlocked.CompareExchange(ref isPublishing, 0, 1);
                    return;
                }

                ResolvedEvent @event;
                while (!stop && queue.TryDequeue(out @event))
                {
                    if (!(@event.OriginalPosition > lastProcessed))
                        continue;

                    var processedEvent = ProcessRawEvent(@event);
                    if (processedEvent != null)
                    {
                        processedEvent.Headers.Set(Constants.CommitPosition, @event.OriginalPosition.Value.CommitPosition.ToString());
                        processedEvent.Headers.Set(Constants.PreparePosition, @event.OriginalPosition.Value.PreparePosition.ToString());
                        eventBus.PublishAsync(processedEvent).GetAwaiter().GetResult();
                    }

                    lastProcessed = @event.OriginalPosition.Value;
                    //TODO: Should be moved to the event handlers to be sure that only when the events are persisted the counters will be updated?
                    eventStorePositionRepository.Save(new EventStorePosition(lastProcessed.CommitPosition, lastProcessed.PreparePosition));
                }

                doneEvent.Set(); // signal end of processing particular queue
                Interlocked.CompareExchange(ref isPublishing, 0, 1);
                // try to reacquire lock if needed
                keepGoing = !stop && queue.Count > 0 && Interlocked.CompareExchange(ref isPublishing, 1, 0) == 0;
            }
        }

        private DomainEvent? ProcessRawEvent(ResolvedEvent rawEvent)
        {
            if (rawEvent.OriginalEvent.Metadata.Length > 0 && rawEvent.OriginalEvent.Data.Length > 0)
                return DeserializeEvent(rawEvent.OriginalEvent.Metadata, rawEvent.OriginalEvent.Data);

            return null;
        }

        /// <summary>
        ///   Deserializes the event from the raw GetEventStore event to my event.
        ///   Took this from a gist that James Nugent posted on the GetEventStore forums.
        /// </summary>
        /// <param name="metadata"></param>
        /// <param name="data"></param>
        /// <returns></returns>
        private DomainEvent? DeserializeEvent(ReadOnlyMemory<byte> metadata, ReadOnlyMemory<byte> data)
        {
            if (JObject.Parse(Encoding.UTF8.GetString(metadata.ToArray())).Property("EventClrTypeName") == null)
                return null;
            var eventClrTypeName = JObject.Parse(Encoding.UTF8.GetString(metadata.ToArray())).Property("EventClrTypeName")!.Value;
            try
            {
                return (DomainEvent?)JsonConvert.DeserializeObject(Encoding.UTF8.GetString(data.ToArray()), Type.GetType((string)eventClrTypeName!)!);
            }
            catch (Exception ex)
            {
                log.LogError(ex.Message);
                return null;
            }
        }
    }
}