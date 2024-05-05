namespace Muflone.Eventstore.gRPC.Persistence
{
    public interface IEventStorePosition
    {
        ulong CommitPosition { get; }
        ulong PreparePosition { get; }
    }


    public class EventStorePosition : IEventStorePosition
    {
        public EventStorePosition(ulong commitPosition, ulong preparePosition)
        {
            CommitPosition = commitPosition;
            PreparePosition = preparePosition;
        }

        public ulong CommitPosition { get; }
        public ulong PreparePosition { get; }
    }
}