using EventStore.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Muflone.Eventstore.gRPC.Persistence;
using Muflone.Persistence;

namespace Muflone.Eventstore.gRPC
{
    public static class EventStoreHelpers
    {
        public static IServiceCollection AddMufloneEventStore(this IServiceCollection services, string evenStoreConnectionString)
        {
            services.AddSingleton(provider =>
            {
                var settings = EventStoreClientSettings.Create(evenStoreConnectionString);
                return new EventStoreClient(settings);
            });
            services.AddScoped<IRepository, EventStoreRepository>();
            services.AddSingleton<IHostedService, EventDispatcher>();
            return services;
        }
    }
}