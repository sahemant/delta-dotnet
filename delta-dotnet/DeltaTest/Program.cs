using Azure.Storage;
using Azure.Storage.Files.DataLake;
using DeltaDotnet;
using Newtonsoft.Json.Linq;
using Parquet.Data.Rows;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeltaTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var watch = new System.Diagnostics.Stopwatch();
            var adlsClient = GetDataLakeServiceClient("storageaccountname", "storageAccountKey");
            var adlsFileSystem = adlsClient.GetFileSystemClient("filesystem");
            var deltaReader = new DeltaReader("ppe/metadatatest/catalog_test", adlsFileSystem);
            watch.Start();
            var table = deltaReader.Read().Result;
            Console.WriteLine(table.Count());

            var array = ToJArray(table);
            Console.WriteLine(array.Count);
            watch.Stop();
            Console.WriteLine($"Execution Time: {watch.ElapsedMilliseconds} ms");
            Console.ReadKey();
        }

        private static JArray ToJArray(Table table)
        {
            var dataFields = table.Schema.GetDataFields();
            var rows = table.AsEnumerable();
            var jObjects = new List<JObject>();
            foreach (var row in rows)
            {
                var jobject = new JObject();
                foreach (var datafield in dataFields)
                {
                    jobject[datafield.Name] = row.GetString(Array.FindIndex(dataFields, x => x.Name == datafield.Name));
                }
                jObjects.Add(jobject);
            }

            return JArray.FromObject(jObjects);
        }

        private static DataLakeServiceClient GetDataLakeServiceClient(string accountName, string accountKey)
        {
            StorageSharedKeyCredential sharedKeyCredential =
                new StorageSharedKeyCredential(accountName, accountKey);

            string dfsUri = "https://" + accountName + ".dfs.core.windows.net";

            return new DataLakeServiceClient
                (new Uri(dfsUri), sharedKeyCredential);
        }
    }
}
