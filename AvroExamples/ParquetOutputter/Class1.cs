using Microsoft.Analytics.Interfaces;
using Microsoft.Analytics.Types.Sql;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;
using Parquet;
using Parquet.Data;

namespace ParquetExamples
{
    [SqlUserDefinedOutputter(AtomicFileProcessing = true)]
    public class ParquetOutputter : IOutputter
    {
        private DataSet _ds;
        private ParquetWriter _writer;
        private MemoryStream _tempStream;
        private Stream _resultStream;
        private WriterOptions writeroptions;

        public ParquetOutputter(int RowGroupSize)
        {
            writeroptions = new WriterOptions() { RowGroupsSize = RowGroupSize };
        }

        public override void Output(IRow input, IUnstructuredWriter output)
        {
            ISchema schema = input.Schema;

            if (_ds == null)
            {
                List<SchemaElement> lse = new List<SchemaElement>();
                for (int i = 0; i < schema.Count(); i++)
                {
                    var col = schema[i];
                    lse.Add(new SchemaElement(col.Name, Type.GetType(getColType(col))));
                }
                _ds = new DataSet(new Schema(lse));

                _tempStream = new MemoryStream();
                _resultStream = output.BaseStream;

                _writer = new ParquetWriter(output.BaseStream, null, writeroptions);

                //create DS based on schema
                //input.Schema
            }

            List<object> ls = new List<object>();
            for (int i = 0; i < schema.Count; i++)
            {
                ls.Add(input.Get<dynamic>(input.Schema[i].Name));
            }
            Row r = new Row(ls);
            _ds.Add(r);
        }
        public override void Close()
        {
            _writer.Write(_ds);
            _writer.Dispose();

            //_tempStream.Position = 0;
            //_tempStream.CopyTo(_resultStream);
        }
   
        string getColType(IColumn col)
        {
            string typeName = string.Empty;
            bool nullable = false;

            if (col.Type.Name.Contains("Nullable"))
            {
                nullable = true;
                typeName = col.Type.FullName.Split('[')[2].Split(',')[0];
            }
            else
                typeName = col.Type.Name;

            if (typeName.Contains("Decimal"))
                typeName = "Double";

            return "System." + typeName.Replace("System.", string.Empty + (nullable ? "?" : ""));
        }

    }

    //[SqlUserDefinedExtractor(AtomicFileProcessing = true)]
    //public class ParquetOutputter : IOutputter
    //{
    //    DataSet _ds;
    //    private ParquetWriter _writer;

    //    public override void Close()
    //    {
    //        _writer.Write(_ds);

    //        if (_writer != null)
    //            _writer.Dispose();
    //    }
    //    public override void Output(IRow input, IUnstructuredWriter output)
    //    {
    //        ISchema schema = input.Schema;

    //        // Note: We don't bloat the JSON with sparse (null) properties
    //        if (_ds == null)
    //        {
    //            List<SchemaElement> lse = new List<SchemaElement>();
    //            for (int i = 0; i < schema.Count(); i++)
    //            {
    //                var col = schema[i];
    //                lse.Add(new SchemaElement(col.Name, Type.GetType(getColType(col))));
    //            }

    //            _writer = new ParquetWriter(output.BaseStream);

    //            _ds = new DataSet(new Schema(lse));
    //        }

    //        List<object> ls = new List<object>();
    //        for (int i = 0; i < schema.Count; i++)
    //        {
    //            ls.Add(input.Get<dynamic>(input.Schema[i].Name));
    //        }
    //        Row r = new Row(ls);
    //        _ds.Add(r);
    //    }

    //    string getColType(IColumn col)
    //    {
    //        string typeName = string.Empty;
    //        bool nullable = false;

    //        if (col.Type.Name.Contains("Nullable"))
    //        {
    //            nullable = true;
    //            typeName = col.Type.FullName.Split('[')[2].Split(',')[0];
    //        }
    //        else
    //            typeName = col.Type.Name;

    //        if (typeName.Contains("Decimal"))
    //            typeName = "Double";

    //        return "System." + typeName.Replace("System.", string.Empty + (nullable ? "?" : ""));
    //    }
    //}
}
