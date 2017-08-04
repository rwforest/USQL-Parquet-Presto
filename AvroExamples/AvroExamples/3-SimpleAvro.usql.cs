using Microsoft.Analytics.Interfaces;
using Microsoft.Analytics.Types.Sql;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Data;
using System.Xml;
using Microsoft.Hadoop.Avro;
using Microsoft.Hadoop.Avro.Container;

namespace AvroExamples
{
    [SqlUserDefinedExtractor(AtomicFileProcessing = true)]
    public class AvroExtractor : IExtractor
    {
        private string avroSchema;

        public AvroExtractor(string avroSchema)
        {
            this.avroSchema = avroSchema;
        }

        public override IEnumerable<IRow> Extract(IUnstructuredReader input, IUpdatableRow output)
        {
            if (input.Length == 0)
            {
                yield break;
            }

            var serializer = AvroSerializer.CreateGeneric(avroSchema);
            using (var genericReader = AvroContainer.CreateGenericReader(input.BaseStream))
            {
                using (var reader = new SequentialReader<dynamic>(genericReader))
                {
                    foreach (var obj in reader.Objects)
                    {
                        foreach (var column in output.Schema)
                        {
                            output.Set(column.Name, obj[column.Name]);
                        }

                        yield return output.AsReadOnly();
                    }
                }
            }
        }
    }
}
