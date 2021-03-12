using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Newtonsoft.Json.Linq;
using Parquet;
using Parquet.Data;
using Parquet.Data.Rows;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace DeltaDotnet
{
    public class DeltaReader
    {
        private string deltaTablePath;
        private DataLakeFileSystemClient dataLakeFileSystemClient;

        public DeltaReader(string deltaTablePath, DataLakeFileSystemClient dataLakeFileSystemClient)
        {
            this.deltaTablePath = deltaTablePath;
            this.dataLakeFileSystemClient = dataLakeFileSystemClient;
        }

        private async Task<IEnumerable<string>> GetParquetFilePaths()
        {

            var parquetFilePaths = new List<string>();
            var deltaLogDirectoryPath = deltaTablePath + "/_delta_log";
            var deltaLogFiles = this.dataLakeFileSystemClient.GetPathsAsync(deltaLogDirectoryPath);
            var deltaLogfilesEnumerator = deltaLogFiles.GetAsyncEnumerator();
            
            await deltaLogfilesEnumerator.MoveNextAsync();
            PathItem item = deltaLogfilesEnumerator.Current;
            while (item != null)
            {
                if(item.Name.EndsWith(".json"))
                {
                    var fileClient = this.dataLakeFileSystemClient.GetFileClient(item.Name);
                    var fileStream = new StreamReader(fileClient.OpenRead());
                    while(!fileStream.EndOfStream)
                    {
                        var logLine = fileStream.ReadLine();
                        var jobject = JObject.Parse(logLine);
                        if (jobject.ContainsKey("add"))
                        {
                            parquetFilePaths.Add(jobject["add"]["path"].ToString());
                        }
                        else if (jobject.ContainsKey("remove"))
                        {
                            parquetFilePaths.RemoveAll(x => x.Equals(jobject["remove"]["path"].ToString()));
                        }
                    }
                }

                if (!await deltaLogfilesEnumerator.MoveNextAsync())
                {
                    break;
                }
                
                item = deltaLogfilesEnumerator.Current;
            }

            return parquetFilePaths;
        }

        private Table ReadParquetFile(string filepath)
        {

            var fileStream = this.dataLakeFileSystemClient.GetFileClient($"{this.deltaTablePath}/{filepath}").OpenRead();
            using (var parquetReader = new ParquetReader(fileStream))
            {
                return parquetReader.ReadAsTable();
            }
        }

        public async Task<Table> Read()
        {
            var filePaths = await this.GetParquetFilePaths();
            var tableSegments = new List<Table>();
            var filteredRows = new List<Row>();
            Schema schema = null;
            foreach(var parquetfilePath in filePaths)
            {
                var table = this.ReadParquetFile(parquetfilePath);
                if (schema == null)
                {
                    schema = table.Schema;
                }
                var dataFields = table.Schema.GetDataFields();
                var nameColumnIndex = Array.FindIndex(dataFields, x => x.Name == "name");
                var rows = table.AsEnumerable();
                filteredRows.AddRange(rows);
            }

            var resultTable = new Table(schema);

            foreach(var row in filteredRows)
            {
                resultTable.Add(row);
            }

            return resultTable;
        }
    }
}
