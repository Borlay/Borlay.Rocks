using System;
using System.Collections.Generic;
using System.Text;

namespace Borlay.Rocks.Database
{
    public static class ByteExtensions
    {
        public static unsafe bool StartsWith(this byte[] left, byte[] right)
        {
            if (left is null || right is null) return left == right;
            if (left.Length < right.Length) return false;

            fixed (byte* str = left)
            {
                byte* chPtr = str;
                fixed (byte* str2 = right)
                {
                    byte* chPtr2 = str2;

                    var seek = 0;
                    while (seek + 8 <= right.Length)
                    {
                        if (*(((long*)chPtr)) != *(((long*)chPtr2)))
                            return false;

                        chPtr += 8;
                        chPtr2 += 8;
                        seek += 8;
                    }

                    while (seek + 4 <= right.Length)
                    {
                        if (*(((int*)chPtr)) != *(((int*)chPtr2)))
                            return false;

                        chPtr += 4;
                        chPtr2 += 4;
                        seek += 4;
                    }

                    while (seek + 1 <= right.Length)
                    {
                        if (*(((byte*)chPtr)) != *(((byte*)chPtr2)))
                            return false;

                        chPtr += 1;
                        chPtr2 += 1;
                        seek += 1;
                    }
                }
            }

            return true;
        }
    }
}
