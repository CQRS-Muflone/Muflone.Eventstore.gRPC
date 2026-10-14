using System.Reflection;
using System.Text;
using EventStore.Client;
using Muflone.Core;
using Muflone.Eventstore.gRPC.Persistence;
using Muflone.Messages.Events;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using Xunit;

namespace Muflone.Eventstore.gRPC.Tests;

/// <summary>
/// Events are read back with Newtonsoft's default settings (EventStoreRepository.DeserializeEvent and
/// EventDispatcher), so writing them with the same contract must not lose anything that is read today:
/// the same event, written the old way and the new way, has to come back in exactly the same state.
/// </summary>
public class EventSerializationTests
{
	private static readonly Dictionary<string, object> Headers = new() { { "CommitId", Guid.NewGuid() } };

	[Fact]
	public void ToEventData_WritesNoBackingFieldsAndNoEqualityContract()
	{
		var json = Data(ToEventData(SampleEvent()));

		Assert.DoesNotContain("k__BackingField", json);
		Assert.DoesNotContain("EqualityContract", json);
		Assert.DoesNotContain("_ticks", json);
	}

	[Fact]
	public void AnEventWrittenTheOldWayAndTheNewWay_ReadsBackInTheSameState()
	{
		var @event = SampleEvent();

		var readFromLegacy = Read(LegacySerializer.Serialize(@event), @event.GetType());
		var readFromNew = Read(Data(ToEventData(@event)), @event.GetType());

		Assert.True(JToken.DeepEquals(FullState(readFromLegacy), FullState(readFromNew)),
			$"legacy: {FullState(readFromLegacy)}\nnew: {FullState(readFromNew)}");
	}

	[Fact]
	public void AnEventWrittenTheNewWay_ReadsBackItsPublicData()
	{
		var @event = SampleEvent();

		var read = (OrderLinesGenerated)Read(Data(ToEventData(@event)), @event.GetType());

		Assert.Equal(@event.AggregateId.Value, read.AggregateId.Value);
		Assert.Equal(@event.Lines, read.Lines);
		Assert.Equal(@event.Note, read.Note);
		Assert.Equal(@event.MessageId, read.MessageId);
		Assert.Equal(@event.Headers.CorrelationId, read.Headers.CorrelationId);
	}

	[Fact]
	public void AnEventWithManyNestedLines_IsAFractionOfTheLegacySize()
	{
		var @event = SampleEvent(lines: 50);

		var legacy = Encoding.UTF8.GetByteCount(LegacySerializer.Serialize(@event));
		var slim = ToEventData(@event).Data.Length;

		Assert.True(slim * 10 < legacy, $"legacy {legacy} bytes, new {slim} bytes");
	}

	[Fact]
	public void ToEventData_KeepsTheHeadersAndTheClrTypeInTheMetadata()
	{
		var @event = SampleEvent();

		var metadata = JObject.Parse(Encoding.UTF8.GetString(ToEventData(@event).Metadata.ToArray()));

		Assert.Equal(@event.GetType().AssemblyQualifiedName, (string?)metadata["EventClrTypeName"]);
		Assert.Equal(Headers["CommitId"].ToString(), (string?)metadata["CommitId"]);
		Assert.Equal(nameof(OrderLinesGenerated), ToEventData(@event).Type);
	}

	[Fact]
	public void AnEventWithNoLines_RoundTrips()
	{
		var @event = SampleEvent(lines: 0);

		var read = (OrderLinesGenerated)Read(Data(ToEventData(@event)), @event.GetType());

		Assert.Empty(read.Lines);
	}

	private static EventData ToEventData(object @event) => EventStoreRepository.ToEventData(Uuid.NewUuid(), @event, Headers);

	private static string Data(EventData eventData) => Encoding.UTF8.GetString(eventData.Data.ToArray());

	// The read path of EventStoreRepository.DeserializeEvent and EventDispatcher.DeserializeEvent.
	private static object Read(string json, Type type) => JsonConvert.DeserializeObject(json, type)!;

	// Every member, public or not: what the object actually holds after it has been read.
	private static JToken FullState(object value) => JToken.Parse(LegacySerializer.Serialize(value));

	private static OrderLinesGenerated SampleEvent(int lines = 3) =>
		new(new OrderId(Guid.NewGuid().ToString()),
			Enumerable.Range(1, lines)
				.Select(i => new OrderLine(Guid.NewGuid(), $"Product {i}",
					new Price(new Money(10.4m * i, 0.42m), new Money(9.36m * i, 0.37m)),
					[new Family(Guid.NewGuid(), "family")]))
				.ToList(),
			"note",
			Guid.NewGuid());
}

/// <summary>The serializer the repository used before: every property and every field, public or not.</summary>
internal static class LegacySerializer
{
	private static readonly JsonSerializerSettings Settings = new()
	{
		TypeNameHandling = TypeNameHandling.None,
		ContractResolver = new PrivateContractResolver()
	};

	public static string Serialize(object value) => JsonConvert.SerializeObject(value, Settings);

	private sealed class PrivateContractResolver : DefaultContractResolver
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

public sealed class OrderId(string value) : DomainId(value);

public sealed record Money(decimal Taxable, decimal Tax)
{
	public decimal Total => Taxable + Tax;
}

public sealed record Price(Money Full, Money? Discounted);

public sealed record Family(Guid Id, string Name);

public sealed record OrderLine(Guid LineId, string Title, Price Price, List<Family> ExcludedFamilies)
{
	public bool Equals(OrderLine? other) =>
		other is not null && LineId == other.LineId && Title == other.Title && Price == other.Price
		&& ExcludedFamilies.SequenceEqual(other.ExcludedFamilies);

	public override int GetHashCode() => LineId.GetHashCode();
}

public sealed class OrderLinesGenerated(OrderId aggregateId, List<OrderLine> lines, string note, Guid correlationId)
	: DomainEvent(aggregateId, correlationId)
{
	public List<OrderLine> Lines { get; } = lines;
	public string Note { get; private set; } = note;
}
