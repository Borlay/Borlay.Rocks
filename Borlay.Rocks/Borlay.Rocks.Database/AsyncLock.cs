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

        public static Task<IDisposable> WaitAsync(params Guid[] guids) => WaitAsync(guids.Select(g => g.GetHashCode()).ToArray());
        public static Task<IDisposable> WaitAsync(params string[] ids) => WaitAsync(ids.Select(id => id.GetHashCode()).ToArray());

        private static int GetIndex(int hashCode) => Math.Abs(hashCode) % locks.Count();

        public static async Task<IDisposable> WaitAsync(params int[] hashCodes)
        {
            var slims = hashCodes.Select(h => GetIndex(h)).Distinct().Select(i => locks[i]).ToArray();
            var tasks = slims.Select(s => s.WaitAsync()).ToArray();
            await Task.WhenAll(tasks);
            var disposes = slims.Select(s => new Action(() => s.Release()));
            return new DisposeAction(disposes.ToArray());
        }
    }

    public class DisposeAction : IDisposable
    {
        private readonly Action[] actions;

        public DisposeAction(params Action[] actions)
        {
            this.actions = actions;
        }

        public void Dispose()
        {
            if (actions == null) return;
            foreach(var action in actions)
            {
                try
                {
                    action.Invoke();
                }
                catch(Exception e)
                {
                    Console.WriteLine(e.ToString());
                }
            }
        }
    }
}
