using System;
using System.Collections.Generic;
using System.Text;

namespace dotnet_sqs_lambda
{
    public class Product
    {
        public string Id { get; set; }
        public string Name { get; set; }

        public decimal? Price { get; set; }
    }
}

