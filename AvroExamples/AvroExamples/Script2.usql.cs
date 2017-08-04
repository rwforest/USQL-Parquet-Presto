using Microsoft.Analytics.Interfaces;
using Microsoft.Hadoop.Avro;
using Microsoft.Hadoop.Avro.Container;
using Microsoft.Hadoop.Avro.Schema;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Serialization;

namespace AvroExamples
{
    [SqlUserDefinedOutputter(AtomicFileProcessing = true)]
    public class AvroOutputter : IOutputter
    {
        private string avroSchema;
        private SequentialWriter<object> writer;
        private IAvroSerializer<AnonType_WeatherDataSet> serializer;

        public AvroOutputter(string avroSchema)
        {
            this.avroSchema = avroSchema;
        }

        public override void Output(IRow row, IUnstructuredWriter output)
        {
            // First Row
            if (this.writer == null)
            {
                //this.writer = new SequentialWriter<object>(AvroContainer.Create<AnonType_WeatherDataSet>(this.avroSchema, output.BaseStream, Codec.Deflate), 24);
                this.serializer = AvroSerializer.Create<AnonType_WeatherDataSet>();

            }

            AnonType_WeatherDataSet data = new AnonType_WeatherDataSet(
                row.Get<string>(row.Schema[0].Name),
                row.Get<int?>(row.Schema[1].Name),
                row.Get<string>(row.Schema[2].Name),
                (double?)row.Get<decimal?>(row.Schema[3].Name),
                row.Get<string>(row.Schema[4].Name),
                row.Get<string>(row.Schema[5].Name),
                row.Get<string>(row.Schema[6].Name),
                row.Get<int?>(row.Schema[7].Name)
                );

            serializer.Serialize(output.BaseStream, data);
        }

        public override void Close()
        {
            if (this.writer != null)
            {
                this.writer.Close();
            }
        }
    }

    [DataContract(Namespace = "")]
    public partial class AnonType_WeatherDataSet
    {
        private const string JsonSchema = @"{""type"":""record"",""name"":""AnonType_WeatherDataSet"",""fields"":[{""name"":""station"",""type"":[""null"",""string""]},{""name"":""datekey"",""type"":[""null"",""int""]},{""name"":""element"",""type"":[""null"",""string""]},{""name"":""value"",""type"":[""null"",""double""]},{""name"":""mflag"",""type"":[""null"",""string""]},{""name"":""qflag"",""type"":[""null"",""string""]},{""name"":""sflag"",""type"":[""null"",""string""]},{""name"":""timekey"",""type"":[""null"",""int""]}]}";

        /// <summary>
        /// Gets the schema.
        /// </summary>
        public static string Schema
        {
            get
            {
                return JsonSchema;
            }
        }

        /// <summary>
        /// Gets or sets the station field.
        /// </summary>
        [DataMember]
        public string station { get; set; }

        /// <summary>
        /// Gets or sets the datekey field.
        /// </summary>
        [DataMember]
        public Nullable<int> datekey { get; set; }

        /// <summary>
        /// Gets or sets the element field.
        /// </summary>
        [DataMember]
        public string element { get; set; }

        /// <summary>
        /// Gets or sets the value field.
        /// </summary>
        [DataMember]
        public Nullable<double> value { get; set; }

        /// <summary>
        /// Gets or sets the mflag field.
        /// </summary>
        [DataMember]
        public string mflag { get; set; }

        /// <summary>
        /// Gets or sets the qflag field.
        /// </summary>
        [DataMember]
        public string qflag { get; set; }

        /// <summary>
        /// Gets or sets the sflag field.
        /// </summary>
        [DataMember]
        public string sflag { get; set; }

        /// <summary>
        /// Gets or sets the timekey field.
        /// </summary>
        [DataMember]
        public Nullable<int> timekey { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="AnonType_WeatherDataSet"/> class.
        /// </summary>
        public AnonType_WeatherDataSet()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AnonType_WeatherDataSet"/> class.
        /// </summary>
        /// <param name="station">The station.</param>
        /// <param name="datekey">The datekey.</param>
        /// <param name="element">The element.</param>
        /// <param name="value">The value.</param>
        /// <param name="mflag">The mflag.</param>
        /// <param name="qflag">The qflag.</param>
        /// <param name="sflag">The sflag.</param>
        /// <param name="timekey">The timekey.</param>
        public AnonType_WeatherDataSet(string station, Nullable<int> datekey, string element, Nullable<double> value, string mflag, string qflag, string sflag, Nullable<int> timekey)
        {
            this.station = station;
            this.datekey = datekey;
            this.element = element;
            this.value = value;
            this.mflag = mflag;
            this.qflag = qflag;
            this.sflag = sflag;
            this.timekey = timekey;
        }
    }
}
