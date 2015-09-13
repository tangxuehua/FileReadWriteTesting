using System;
using System.Collections.Concurrent;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace FileReadWriteTesting
{
    class Program
    {
        static ConcurrentQueue<FileReader> _readerDict = new ConcurrentQueue<FileReader>();
        static long _writtenCount;
        static long _readCount;
        static long _basePosition;
        static Stopwatch _watch;

        static void Main(string[] args)
        {
            var file = ConfigurationManager.AppSettings["file"];
            var option = ConfigurationManager.AppSettings["option"];

            if (option == "ConcurrentWrite")
            {
                ConcurrentWriteFile(file);
            }
            else if (option == "ConcurrentRead")
            {
                ConcurrentReadFile(file);
            }
            else if (option == "ParallelRead")
            {
                ParallelReadFile(file);
            }

            Console.ReadLine();
        }

        /// <summary>多线程并发的顺序写同一个文件；此测试用例是模拟EQueue中多线程并发写消息到同一个消息文件的场景。
        /// </summary>
        /// <param name="file"></param>
        static void ConcurrentWriteFile(string file)
        {
            var threadCount = int.Parse(ConfigurationManager.AppSettings["concurrentThreadCount"]);     //并行读取文件的线程数
            var messageSize = int.Parse(ConfigurationManager.AppSettings["messageSize"]);               //一次读取的单个消息的大小，字节为单位
            var messageCount = int.Parse(ConfigurationManager.AppSettings["messageCount"]);             //总共要读取的消息的个数
            var message = new byte[messageSize];
            for (var i = 0; i < messageSize; i++)
            {
                message[i] = 1;
            }
            var lockObject = new object();
            var fileStream = new FileStream(file, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite, 8192, FileOptions.SequentialScan);
            var writer = new BinaryWriter(fileStream);

            _watch = Stopwatch.StartNew();

            //多线程并发写文件，用锁，确保同一时刻只有一个线程在写文件
            for (var i = 0; i < threadCount; i++)
            {
                Task.Factory.StartNew(() =>
                {
                    while (_writtenCount < messageCount)
                    {
                        lock (lockObject)
                        {
                            //写文件
                            writer.Write(message);

                            //递增写入次数，并判断是否要打印
                            _writtenCount++;

                            //每写入100次，刷一次磁盘；实际EQueue是采用异步定时刷盘，定时间隔默认是1s
                            if (_writtenCount % 100 == 0)
                            {
                                fileStream.Flush();
                            }

                            //判断是否要打印
                            if (_writtenCount % 100000 == 0)
                            {
                                Console.WriteLine("Written {0} messages, position: {1}, timeSpent: {2}ms", _writtenCount, fileStream.Position, _watch.ElapsedMilliseconds);
                            }
                        }
                    }
                });
            }
        }
        /// <summary>多线程并发的顺序读同一个文件；此测试用例是模拟EQueue中多线程读消息索引文件的场景。
        /// </summary>
        /// <param name="file"></param>
        static void ConcurrentReadFile(string file)
        {
            var threadCount = int.Parse(ConfigurationManager.AppSettings["concurrentThreadCount"]);     //并行读取文件的线程数
            var messageSize = int.Parse(ConfigurationManager.AppSettings["messageSize"]);               //一次读取的单个消息的大小，字节为单位
            var messageCount = int.Parse(ConfigurationManager.AppSettings["messageCount"]);             //总共要读取的消息的个数
            var message = new byte[messageSize];
            var fileReader = CreateFileReader(file, FileOptions.SequentialScan);
            var lockObject = new object();

            _watch = Stopwatch.StartNew();

            //多线程并发读文件
            for (var i = 0; i < threadCount; i++)
            {
                Task.Factory.StartNew(() =>
                {
                    while (_readCount < messageCount)
                    {
                        lock (lockObject)
                        {
                            //顺序读文件
                            fileReader.Reader.Read(message, 0, message.Length);

                            //递增读取次数，并判断是否要打印
                            _readCount++;
                            if (_readCount % 100000 == 0)
                            {
                                Console.WriteLine("Read {0} messages, position: {1}, timeSpent: {2}ms", _readCount, fileReader.FileStream.Position, _watch.ElapsedMilliseconds);
                            }
                        }
                    }
                });
            }
        }
        /// <summary>多线程并行的读同一个文件，读的位置总体是在往前移动，但并不是完全顺序向前读取；此测试用例是模拟EQueue中多线程读消息文件的场景。
        /// </summary>
        /// <param name="file"></param>
        static void ParallelReadFile(string file)
        {
            var fileReaderCount = int.Parse(ConfigurationManager.AppSettings["fileReaderCount"]);       //读文件的BinaryReader的个数，BinaryReader可复用
            var threadCount = int.Parse(ConfigurationManager.AppSettings["concurrentThreadCount"]);     //并行读取文件的线程数
            var messageSize = int.Parse(ConfigurationManager.AppSettings["messageSize"]);               //一次读取的单个消息的大小，字节为单位
            var messageCount = int.Parse(ConfigurationManager.AppSettings["messageCount"]);             //总共要读取的消息的个数

            //先初始化好所有的BinaryReader实例
            InitializeFileReaders(file, fileReaderCount, FileOptions.RandomAccess);

            _watch = Stopwatch.StartNew();

            //多线程并行读取文件
            for (var i = 0; i < threadCount; i++)
            {
                Task.Factory.StartNew(() =>
                {
                    var offsetMax = 10000;
                    var offsetMin = -10000;
                    var message = new byte[messageSize];
                    var random = new Random();

                    while (_readCount < messageCount)
                    {
                        var basePosition = Interlocked.Increment(ref _basePosition);
                        var offset = random.Next(offsetMin, offsetMax);
                        var position = basePosition + offset;
                        if (position < 0)
                        {
                            position = 0;
                        }
                        else if (position >= messageCount)
                        {
                            position = position - messageSize;
                        }
                        ReadFile(message, position);
                    }
                });
            }
        }

        static void ReadFile(byte[] message, long position)
        {
            var fileReader = GetFileReader();
            try
            {
                //指定当前要读取的位置
                fileReader.FileStream.Position = position;

                //读文件
                fileReader.Reader.Read(message, 0, message.Length);

                //递增读取次数，并判断是否要打印
                var count = Interlocked.Increment(ref _readCount);
                if (count % 100000 == 0)
                {
                    Console.WriteLine("Read {0} messages, position: {1}, timeSpent: {2}ms", count, position, _watch.ElapsedMilliseconds);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                ReturnFileReader(fileReader);
            }
        }
        static void InitializeFileReaders(string file, int readerCount, FileOptions options)
        {
            for (var i = 0; i < readerCount; i++)
            {
                _readerDict.Enqueue(CreateFileReader(file, options));
            }
        }
        static FileReader CreateFileReader(string file, FileOptions options)
        {
            var fileStream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.Read, 8192, options);
            var reader = new BinaryReader(fileStream);
            return new FileReader(fileStream, reader);
        }
        static FileReader GetFileReader()
        {
            FileReader reader;
            while (!_readerDict.TryDequeue(out reader))
            {
                Thread.Sleep(1);
            }
            return reader;
        }
        static void ReturnFileReader(FileReader reader)
        {
            _readerDict.Enqueue(reader);
        }
        class FileReader
        {
            public FileStream FileStream;
            public BinaryReader Reader;

            public FileReader(FileStream fileStream, BinaryReader reader)
            {
                FileStream = fileStream;
                Reader = reader;
            }
        }
    }
}
