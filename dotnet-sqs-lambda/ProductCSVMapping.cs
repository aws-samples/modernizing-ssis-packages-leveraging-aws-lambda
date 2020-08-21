using System;
using System.Collections.Generic;
using System.Text;
using TinyCsvParser.Mapping;

namespace dotnet_sqs_lambda
{
    public class CsvProductMapping : CsvMapping<Product>
    {
        public CsvProductMapping() : base()
        {
            MapProperty(0, x => x.Id);
            MapProperty(1, x => x.Name);
            MapProperty(2, x => x.Price);            
        }
    }
}
