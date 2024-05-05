namespace Muflone.Eventstore.gRPC.Persistence
{
    public interface IEventStorePosition
    {
        long CommitPosition { get; }
        long PreparePosition { get; }
    }


    public class EventStorePosition : IEventStorePosition
    {
        public EventStorePosition(long commitPosition, long preparePosition)
        {
            CommitPosition = commitPosition;
            PreparePosition = preparePosition;
        }

        public long CommitPosition { get; }
        public long PreparePosition { get; }
    }
}