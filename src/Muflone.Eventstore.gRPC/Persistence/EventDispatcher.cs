using EventStore.Client;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Muflone.Messages.Events;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Text;

namespace Muflone.Eventstore.gRPC.Persistence
{
	public class EventDispatcher : IHostedService
	{
		private readonly IEventBus eventBus;
		private readonly EventStoreClient eventStoreClient;
		private readonly IEventStorePositionRepository eventStorePositionRepository;
		private readonly ILogger log;
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

			//No await or it will never give back control and the app won't start
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
			SubscribeToAllAsync(lastProcessed, cancellationToken);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
		}

		public Task StopAsync(CancellationToken cancellationToken)
		{
			if (cancellationToken.IsCancellationRequested)
				return Task.FromCanceled(cancellationToken);

			stop = true;

			log.LogInformation("EventDispatcher stopped");

			return Task.CompletedTask;
		}

		private async Task SubscribeToAllAsync(Position from, CancellationToken cancellationToken)
		{
		Subscription:
			try
			{
				await using var subscription = eventStoreClient.SubscribeToAll(FromAll.After(from), cancellationToken: cancellationToken);
				await foreach (var message in subscription.Messages)
				{
					if (stop)
						break;
					if (message is StreamMessage.Event(var evnt))
					{
						log.LogDebug($"Received event {evnt.OriginalEventNumber}@{evnt.OriginalStreamId}");
						await PublishEvent(evnt);
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
				goto Subscription;
			}
		}

		private async Task PublishEvent(ResolvedEvent resolvedEvent)
		{
			if (!(resolvedEvent.OriginalPosition > lastProcessed))
				return;

			var processedEvent = ProcessRawEvent(resolvedEvent);
			if (processedEvent != null)
			{
				processedEvent.Headers.Set(Constants.CommitPosition, resolvedEvent.OriginalPosition.Value.CommitPosition.ToString());
				processedEvent.Headers.Set(Constants.PreparePosition, resolvedEvent.OriginalPosition.Value.PreparePosition.ToString());
				await eventBus.PublishAsync(processedEvent);
			}

			lastProcessed = resolvedEvent.OriginalPosition.Value;
			await eventStorePositionRepository.Save(new EventStorePosition(lastProcessed.CommitPosition, lastProcessed.PreparePosition));
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