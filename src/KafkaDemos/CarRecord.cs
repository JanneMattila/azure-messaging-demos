using Avro;
using Avro.Specific;
using System.Text.Json.Serialization;

namespace KafkaDemos;

public class CarRecord : ISpecificRecord
{
    public const string SchemaText = @"
       {
  ""type"": ""record"",
  ""name"": ""CarRecord"",
  ""namespace"": ""com.jannemattila"",
  ""fields"": [
    {
      ""name"": ""carid"",
      ""type"": ""string""
    },
    {
      ""name"": ""model"",
      ""type"": ""string""
    },
    {
      ""name"": ""manufacturer"",
      ""type"": ""string""
    },
    {
      ""name"": ""year"",
      ""type"": ""long""
    }
  ]
}";
    public static Schema _SCHEMA = Schema.Parse(SchemaText);

    [JsonIgnore]
    public virtual Schema Schema => _SCHEMA;

    public string CarID { get; set; }
    public string Model { get; set; }
    public string Manufacturer { get; set; }
    public long Year { get; set; }

    public virtual object Get(int fieldPos)
    {
        switch (fieldPos)
        {
            case 0: return this.CarID;
            case 1: return this.Model;
            case 2: return this.Manufacturer;
            case 3: return this.Year;
            default: throw new AvroRuntimeException($"Bad index {fieldPos} in Get()");
        };
    }
    public virtual void Put(int fieldPos, object fieldValue)
    {
        switch (fieldPos)
        {
            case 0: this.CarID = (string)fieldValue; break;
            case 1: this.Model = (string)fieldValue; break;
            case 2: this.Manufacturer = (string)fieldValue; break;
            case 3: this.Year = (long)fieldValue; break;
            default: throw new AvroRuntimeException($"Bad index {fieldPos} in Put()");
        };
    }
}
