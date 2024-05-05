using EventStore.Client;
using Muflone.Core;
using Muflone.Persistence;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using System.Reflection;
using System.Text;

//TODO: Move from newtonsoft,json to system.text.json
//TODO: Update Muflone IRepository to implement CancellationToken
//TODO: Update Muflone IRepository to implement long instead of int for version

namespace Muflone.Eventstore.gRPC.Persistence
{
    public class EventStoreRepository : IRepository
    {
        private const string EventClrTypeHeader = "EventClrTypeName";
        private const string AggregateClrTypeHeader = "AggregateClrTypeName";
        private const string CommitIdHeader = "CommitId";
        private const string CommitDateHeader = "CommitDate";
        private const int WritePageSize = 500;
        private const int ReadPageSize = 500;

        private readonly Func<Type, Guid, string> aggregateIdToStreamName;

        private readonly EventStoreClient eventStoreClient;
        private static readonly JsonSerializerSettings SerializerSettings;

        static EventStoreRepository()
        {
            SerializerSettings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.None,
                ContractResolver = new PrivateContractResolver()
            };
        }

        //This rename is needed to be consistent with naming convention of EventStore javascript
        public EventStoreRepository(EventStoreClient eventStoreClient)
          //: this(eventStoreClient, (type, aggregateId) => $"{char.ToLower(type.Name[0]) + type.Name.Substring(1)}-{aggregateId}")
          : this(eventStoreClient, (type, aggregateId) => $"{char.ToLower(type.Name[0]) + type.Name[1..]}-{aggregateId}")
        {

        }

        public EventStoreRepository(EventStoreClient eventStoreClient, Func<Type, Guid, string> aggregateIdToStreamName)
        {
            this.eventStoreClient = eventStoreClient;
            this.aggregateIdToStreamName = aggregateIdToStreamName;
        }

        public async Task<TAggregate?> GetByIdAsync<TAggregate>(Guid id/*, CancellationToken cancellationToken = default*/) where TAggregate : class, IAggregate
        {
            return await GetByIdAsync<TAggregate>(id, int.MaxValue /*, cancellationToken*/);
        }

        public async Task<TAggregate?> GetByIdAsync<TAggregate>(Guid id, int version/*, CancellationToken cancellationToken = default*/) where TAggregate : class, IAggregate
        {
            if (version <= 0)
                throw new InvalidOperationException("Cannot get version <= 0");

            var streamName = aggregateIdToStreamName(typeof(TAggregate), id);
            var aggregate = ConstructAggregate<TAggregate>();

            var readResult = eventStoreClient.ReadStreamAsync(Direction.Forwards, streamName, StreamPosition.Start, maxCount: version  /*, cancellationToken: cancellationToken*/);

            if (await readResult.ReadState != ReadState.Ok)
                throw new AggregateNotFoundException(id, typeof(TAggregate));

            await readResult.Select(DeserializeEvent).ForEachAsync(@event => aggregate!.ApplyEvent(@event)/*, cancellationToken*/);
            //await foreach (var @event in readResult)
            //    aggregate!.ApplyEvent(DeserializeEvent(@event));

            if (aggregate!.Version != version && version < int.MaxValue)
                throw new AggregateVersionException(id, typeof(TAggregate), aggregate.Version, version);

            return aggregate;
        }

        private static TAggregate? ConstructAggregate<TAggregate>()
        {
            return (TAggregate)Activator.CreateInstance(typeof(TAggregate), true)!;
        }

        private static object DeserializeEvent(ResolvedEvent resolvedEvent)
        {
            var eventClrTypeName = JObject.Parse(Encoding.UTF8.GetString(resolvedEvent.Event.Metadata.ToArray())).Property(EventClrTypeHeader)!.Value;
            return JsonConvert.DeserializeObject(Encoding.UTF8.GetString(resolvedEvent.Event.Data.ToArray()), Type.GetType(((string)eventClrTypeName)!)!)!;
        }

        public async Task SaveAsync(IAggregate aggregate, Guid commitId, Action<IDictionary<string, object>> updateHeaders)
        {
            var commitHeaders = new Dictionary<string, object>
              {
                { CommitIdHeader, commitId },
                { CommitDateHeader, DateTime.UtcNow},
                { AggregateClrTypeHeader, aggregate.GetType().AssemblyQualifiedName! }
              };
            updateHeaders(commitHeaders);

            var streamName = aggregateIdToStreamName(aggregate.GetType(), aggregate.Id.Value);
            var newEvents = aggregate.GetUncommittedEvents().Cast<object>().ToList();
            var originalVersion = aggregate.Version - newEvents.Count;
            var expectedVersion = originalVersion == 0 ? ExpectedVersion.NoStream : originalVersion - 1;
            var eventsToSave = newEvents.Select(e => ToEventData(Guid.NewGuid(), e, commitHeaders)).ToList();

            if (eventsToSave.Count < WritePageSize)
            {
                eventStoreClient.AppendToStreamAsync(streamName, expectedVersion, eventsToSave).Wait();
            }
            else
            {
                var transaction = eventStoreClient.StartTransactionAsync(streamName, expectedVersion).Result;

                var position = 0;
                while (position < eventsToSave.Count)
                {
                    var pageEvents = eventsToSave.Skip(position).Take(WritePageSize);
                    await transaction.WriteAsync(pageEvents);
                    position += WritePageSize;
                }
                await transaction.CommitAsync();
                transaction.Dispose();
            }
            aggregate.ClearUncommittedEvents();
        }

        public async Task SaveAsync(IAggregate aggregate, Guid commitId)
        {
            await SaveAsync(aggregate, commitId, h => { });
        }

        private static EventData ToEventData(Guid eventId, object @event, IDictionary<string, object> headers)
        {
            var data = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(@event, SerializerSettings));
            var eventHeaders = new Dictionary<string, object>(headers) { { EventClrTypeHeader, @event.GetType().AssemblyQualifiedName! } };
            var metadata = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(eventHeaders, SerializerSettings));
            var typeName = @event.GetType().Name;
            return new EventData(eventId, typeName, true, data, metadata);
        }

        #region IDisposable Support
        private bool disposedValue; // To detect redundant calls
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                }
                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.
                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~EventStoreRepository() {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }

    internal class PrivateContractResolver : DefaultContractResolver
    {
        protected override IList<JsonProperty> CreateProperties(Type type, MemberSerialization memberSerialization)
        {
            var props = type.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
              .Select(p => base.CreateProperty(p, memberSerialization))
              .Union(type.GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
              .Select(f => base.CreateProperty(f, memberSerialization)))
              .ToList();
            props.ForEach(p => { p.Writable = true; p.Readable = true; });
            return props;
        }
    }

}