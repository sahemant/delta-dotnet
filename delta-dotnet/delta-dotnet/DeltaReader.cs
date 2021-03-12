using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DeltaDotnet
{
    public class DeltaReader
    {
        private string deltaTablePath;

        public DeltaReader(string deltaTablePath)
        {
            this.deltaTablePath = deltaTablePath;
        }

        private IEnumerable<string> GetParquetFilePaths()
        {
            var directories = Directory.EnumerateDirectories(this.deltaTablePath);
            var deltaLogDirectory = directories.FirstOrDefault(x => x.Contains("_delta_log"));
            var logFiles = Directory.EnumerateFiles(deltaLogDirectory, "*.json");
            return logFiles;
        }

        public void Read()
        {
            var filePaths = this.GetParquetFilePaths();
        }
    }
}
