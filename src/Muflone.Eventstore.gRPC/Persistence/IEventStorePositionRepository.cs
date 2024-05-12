namespace Muflone.Eventstore.gRPC.Persistence
{
	public interface IEventStorePositionRepository
	{
		Task<IEventStorePosition> GetLastPosition();
		Task Save(IEventStorePosition position);
	}
}