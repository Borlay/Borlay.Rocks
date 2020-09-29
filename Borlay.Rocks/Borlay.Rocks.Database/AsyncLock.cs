using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Borlay.Rocks.Database
{
    public static class AsyncLock
    {
        private static Dictionary<int, SemaphoreSlim> locks;

        static AsyncLock()
        {
            locks = Enumerable.Range(0, 10000).ToDictionary(r => r, r => new SemaphoreSlim(1, 1));
        }

        public static Task<IDisposable> WaitAsync(Guid guid) => WaitAsync(guid.GetHashCode());
        public static Task<IDisposable> WaitAsync(string id) => WaitAsync(id.GetHashCode());

        //private static SemaphoreSlim GetLock(this string key) => 
        //{
        //    var code = Math.Abs(key.GetHashCode()) % locks.Count();
        //    var slim = locks[code];
        //    return slim;
        //}

        private static SemaphoreSlim GetLock(this int hashCode)
        {
            var code = Math.Abs(hashCode) % locks.Count();
            var slim = locks[code];
            return slim;
        }

        public static async Task<IDisposable> WaitAsync(this int hashCode)
        {
            var slim = GetLock(hashCode);
            await slim.WaitAsync();
            return new DisposeAction(() => slim.Release());
        }
    }

    public class DisposeAction : IDisposable
    {
        private readonly Action action;

        public DisposeAction(Action action)
        {
            this.action = action;
        }

        public void Dispose()
        {
            action?.Invoke();
        }
    }
}
