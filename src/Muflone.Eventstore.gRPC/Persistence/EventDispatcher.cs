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
        private const int ThreadKillTimeoutMillisec = 5000;

        private readonly IEventBus eventBus;
        private readonly EventStoreClient eventStoreClient;
        private readonly IEventStorePositionRepository eventStorePositionRepository;
        private readonly ManualResetEventSlim liveDone = new(true);
        private readonly ConcurrentQueue<ResolvedEvent> liveQueue = new();
        private readonly ILogger log;
        private int isPublishing;
        private Position lastProcessed;
        private volatile bool stop;

        public EventDispatcher(ILoggerFactory loggerFactory, EventStoreClient store, IEventBus eventBus, IEventStorePositionRepository eventStorePositionRepository)
        {
            log = loggerFactory?.CreateLogger(GetType()) ?? throw new ArgumentNullException(nameof(loggerFactory));
            eventStoreClient = store ?? throw new ArgumentNullException(nameof(store));
            this.eventBus = eventBus ?? throw new ArgumentNullException(nameof(eventBus));
            this.eventStorePositionRepository = eventStorePositionRepository;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
                cancellationToken.ThrowIfCancellationRequested();
            log.LogInformation("EventDispatcher started");

            var position = await eventStorePositionRepository.GetLastPosition();
            lastProcessed = (position != null) ? new Position(position.CommitPosition, position.PreparePosition) : Position.Start;

            await SubscribeToAllAsync(/* nextPos*/ lastProcessed, cancellationToken);
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
                return Task.FromCanceled(cancellationToken);

            stop = true;

            if (!liveDone.Wait(ThreadKillTimeoutMillisec))
                throw new TimeoutException("EventDispatchStoppingException");

            log.LogInformation("EventDispatcher stopped");

            return Task.CompletedTask;
        }

        private async Task SubscribeToAllAsync(Position from, CancellationToken cancellationToken)
        {
            try
            {
                var subscription = eventStoreClient.SubscribeToAll(FromAll.After(from), cancellationToken: cancellationToken);
                await foreach (var message in subscription.Messages)
                {
                    if (stop)
                        break;
                    if (message is StreamMessage.Event(var evnt))
                    {
                        log.LogDebug($"Received event {evnt.OriginalEventNumber}@{evnt.OriginalStreamId}");
                        liveQueue.Enqueue(evnt);
                        EnsurePublishEvents(liveQueue, liveDone);
                    }
                }
            }
            catch (OperationCanceledException ex)
            {
                log.LogInformation(ex, "Subscription was canceled");
            }
            catch (ObjectDisposedException ex)
            {
                log.LogInformation(ex, "Subscription was canceled by the user");
            }
            catch (Exception ex)
            {
                log.LogError($"Subscription was dropped: {ex.Message}", ex);
                throw;
            }
        }

        private void EnsurePublishEvents(ConcurrentQueue<ResolvedEvent> queue, ManualResetEventSlim doneEvent)
        {
            if (stop)
                return;

            if (Interlocked.CompareExchange(ref isPublishing, 1, 0) == 0)
                ThreadPool.QueueUserWorkItem(_ => PublishEvents(queue, doneEvent));
        }

        private void PublishEvents(ConcurrentQueue<ResolvedEvent> queue, ManualResetEventSlim doneEvent)
        {
            var keepGoing = true;
            while (keepGoing)
            {
                doneEvent.Reset();
                if (stop)
                {
                    doneEvent.Set();
                    Interlocked.CompareExchange(ref isPublishing, 0, 1);
                    return;
                }

                while (!stop && queue.TryDequeue(out var @event))
                {
                    if (!(@event.OriginalPosition > lastProcessed))
                        continue;

                    var processedEvent = ProcessRawEvent(@event);
                    if (processedEvent != null)
                    {
                        processedEvent.Headers.Set(Constants.CommitPosition, @event.OriginalPosition.Value.CommitPosition.ToString());
                        processedEvent.Headers.Set(Constants.PreparePosition, @event.OriginalPosition.Value.PreparePosition.ToString());
                        eventBus.PublishAsync(processedEvent);
                    }

                    lastProcessed = @event.OriginalPosition.Value;
                    //TODO: Should be moved inside the event handlers to be sure that only when events are persisted the counter will be updated?
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