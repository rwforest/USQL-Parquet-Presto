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
        private IAvroSerializer<object> serializer;

        public AvroOutputter(string avroSchema)
        {
            this.avroSchema = avroSchema;
        }

        public override void Output(IRow row, IUnstructuredWriter output)
        {
            // First Row
            if (this.writer == null)
            {
                this.writer = new SequentialWriter<object>(AvroContainer.CreateGenericWriter(this.avroSchema, output.BaseStream, Codec.Deflate), 24);
                this.serializer = AvroSerializer.CreateGeneric(this.avroSchema);
            }

            dynamic data = new AvroRecord(this.serializer.WriterSchema);

            data.station = row.Get<string>(row.Schema[0].Name);
            data.datekey = row.Get<int?>(row.Schema[1].Name);
            data.element = row.Get<string>(row.Schema[2].Name);
            data.value = (double?)row.Get<decimal?>(row.Schema[3].Name);
            data.mflag = row.Get<string>(row.Schema[4].Name);
            data.qflag = row.Get<string>(row.Schema[5].Name);
            data.sflag = row.Get<string>(row.Schema[6].Name);
            data.timekey = row.Get<int?>(row.Schema[7].Name);

            writer.Write(data);
        }

        public override void Close()
        {
            if (this.writer != null)
            {
                this.writer.Close();
            }
        }
    }
}