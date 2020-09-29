using Borlay.Rocks.Database;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace Borlay.Rocks.Tests
{
    [TestClass]
    public class RocksTests
    {
        private readonly RocksRepository repository;

        public RocksTests()
        {
            var builde = new DatabaseBuilder(@"C:\rocks\tests", 0, 1, true, RocksDbSharp.Recovery.AbsoluteConsistency, true);
            repository = builde.AddColumnFamily("test-column", true, false).CreateRepository();
        }

        [TestMethod]
        public void TestMethod1()
        {
            //repository.
        }
    }

    public class Record : ISortableRecord
    {
        public long Position { get; set; }

        public Guid Id { get; set; }

        public string Value { get; set; }
    }
}
