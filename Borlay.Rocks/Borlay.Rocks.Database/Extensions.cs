using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Borlay.Rocks.Database
{
    public static class Extensions
    {
        public static string CreateDirectory(this string directory, params string[] folders)
        {
            var paths = new List<string>();
            paths.Add(directory);
            paths.AddRange(folders);

            var path = Path.Combine(paths.ToArray());

            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            return path;
        }

        public static byte[] ToByteArray(this string value)
        {
            return Encoding.UTF8.GetBytes(value);
        }

        public static byte[] Concat(this byte[] bytes, params string[] values)
        {
            var value = string.Join("", values);
            var vb = Encoding.UTF8.GetBytes(value);
            return bytes.Concat(vb).ToArray();
        }

        public static byte[] Concat(this byte[] bytes, params byte[] concatBytes)
        {
            return bytes.Concat((IEnumerable<byte>)concatBytes).ToArray();
        }

        public static byte[] Concat(this byte[] bytes, byte[] concatBytes, int count)
        {
            var newConcatBytes = new byte[bytes.Length + count];
            Array.Copy(bytes, 0, newConcatBytes, 0, bytes.Length);
            Array.Copy(concatBytes, 0, newConcatBytes, bytes.Length, count);

            return newConcatBytes;
        }

        public static byte[] ToBytesByDescending(this DateTime date)
        {
            var time = date.ToFileTimeUtc();
            return time.ToBytesByDescending();
        }

        public static byte[] ToBytesByAscending(this DateTime date)
        {
            var time = date.ToFileTimeUtc();
            return time.ToBytesByAscending();
        }

        public static byte[] ToBytesByDescending(this long time)
        {
            var descTime = long.MaxValue - time;
            var timeBytes = BitConverter.GetBytes(descTime);
            var descTimeBytes = BitConverter.IsLittleEndian ? timeBytes.Reverse().ToArray() : timeBytes;
            return descTimeBytes;
        }

        public static byte[] ToBytesByAscending(this long time)
        {
            var descTime = time;
            var timeBytes = BitConverter.GetBytes(descTime);
            var descTimeBytes = BitConverter.IsLittleEndian ? timeBytes.Reverse().ToArray() : timeBytes;
            return descTimeBytes;
        }

        public static byte[] ToByteArray(this Guid value, int count)
        {
            var bytes = value.ToByteArray();
            if (count == bytes.Length) return bytes;

            Array.Resize(ref bytes, count);
            return bytes;
        }

        public static long ToLong(this byte[] bytes)
        {
            bytes = BitConverter.IsLittleEndian ? bytes.Reverse().ToArray() : bytes;
            var value = BitConverter.ToInt64(bytes, 0);
            return value;
        }

        public static long ToLongByDescending(this byte[] bytes)
        {
            bytes = BitConverter.IsLittleEndian ? bytes.Reverse().ToArray() : bytes;
            var value = long.MaxValue - BitConverter.ToInt64(bytes, 0);
            return value;
        }
    }
}
