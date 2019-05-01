using System;
using System.IO;
using System.Security.Cryptography;

namespace GlacierTools
{
    class HashUtil
    {

        static int ONE_MB = 1024 * 1024;

        /**
         * Computes the SHA-256 tree hash for the given file
         * 
         * @param inputFile
         *            A file to compute the SHA-256 tree hash for
         * @return a byte[] containing the SHA-256 tree hash
         */
        public static byte[] ComputeSHA256TreeHash(Stream inputFile)
        {
            byte[][] chunkSHA256Hashes = GetChunkSHA256Hashes(inputFile);
            return ComputeSHA256TreeHash(chunkSHA256Hashes);
        }

        public static byte[] ComputeSHA256TreeHash(byte[] buffer, int len)
        {
            byte[][] chunkSHA256Hashes = GetChunkSHA256Hashes(buffer, len);
            return ComputeSHA256TreeHash(chunkSHA256Hashes);
        }


        /**
         * Computes a SHA256 checksum for each 1 MB chunk of the input file. This
         * includes the checksum for the last chunk even if it is smaller than 1 MB.
         * 
         * @param file
         *            A file to compute checksums on
         * @return a byte[][] containing the checksums of each 1MB chunk
         */
        public static byte[][] GetChunkSHA256Hashes(Stream file)
        {
            var sha256 = System.Security.Cryptography.SHA256.Create();

            long numChunks = file.Length / ONE_MB;
            if (file.Length % ONE_MB > 0)
            {
                numChunks++;
            }

            if (numChunks == 0)
            {
                return new byte[][] { CalculateSHA256Hash(sha256, new byte[] { }, 0) };
            }
            byte[][] chunkSHA256Hashes = new byte[(int)numChunks][];

            byte[] buff = new byte[ONE_MB];

            int bytesRead;
            int idx = 0;

            while ((bytesRead = file.Read(buff, 0, ONE_MB)) > 0)
            {
                chunkSHA256Hashes[idx++] = CalculateSHA256Hash(sha256, buff, bytesRead);
            }
            return chunkSHA256Hashes;
        }

        public static byte[][] GetChunkSHA256Hashes(byte[] buffer, int len)
        {
            var sha256 = System.Security.Cryptography.SHA256.Create();

            long numChunks = len / ONE_MB;
            if (len % ONE_MB > 0)
                numChunks++;

            if (numChunks == 0)
                return new byte[][] { CalculateSHA256Hash(sha256, new byte[] { }, 0) };

            byte[][] chunkSHA256Hashes = new byte[(int)numChunks][];

            byte[] buff = new byte[ONE_MB];

            int idx = 0;

            int bytesLeft = len;
            int pos = 0;
            while (bytesLeft > 0)
            {
                int bytesRead = Math.Min(bytesLeft, ONE_MB);
                Array.Copy(buffer, pos, buff, 0, bytesRead);
                chunkSHA256Hashes[idx++] = CalculateSHA256Hash(sha256, buff, bytesRead);
                pos += bytesRead;
                bytesLeft -= bytesRead;
            }

            return chunkSHA256Hashes;
        }

        /**
         * Computes the SHA-256 tree hash for the passed array of 1MB chunk
         * checksums.
         * 
         * This method uses a pair of arrays to iteratively compute the tree hash
         * level by level. Each iteration takes two adjacent elements from the
         * previous level source array, computes the SHA-256 hash on their
         * concatenated value and places the result in the next level's destination
         * array. At the end of an iteration, the destination array becomes the
         * source array for the next level.
         * 
         * @param chunkSHA256Hashes
         *            An array of SHA-256 checksums
         * @return A byte[] containing the SHA-256 tree hash for the input chunks
         */
        public static byte[] ComputeSHA256TreeHash(byte[][] chunkSHA256Hashes)
        {
            var sha256 = System.Security.Cryptography.SHA256.Create();

            byte[][] prevLvlHashes = chunkSHA256Hashes;
            while (prevLvlHashes.GetLength(0) > 1)
            {

                int len = prevLvlHashes.GetLength(0) / 2;
                if (prevLvlHashes.GetLength(0) % 2 != 0)
                    len++;

                byte[][] currLvlHashes = new byte[len][];

                int j = 0;
                for (int i = 0; i < prevLvlHashes.GetLength(0); i = i + 2, j++)
                {
                    // If there are at least two elements remaining
                    if (prevLvlHashes.GetLength(0) - i > 1)
                    {
                        // Calculate a digest of the concatenated nodes
                        byte[] firstPart = prevLvlHashes[i];
                        byte[] secondPart = prevLvlHashes[i + 1];
                        byte[] concatenation = new byte[firstPart.Length + secondPart.Length];
                        System.Buffer.BlockCopy(firstPart, 0, concatenation, 0, firstPart.Length);
                        System.Buffer.BlockCopy(secondPart, 0, concatenation, firstPart.Length, secondPart.Length);

                        currLvlHashes[j] = CalculateSHA256Hash(sha256, concatenation, concatenation.Length);

                    }
                    else
                    { // Take care of remaining odd chunk
                        currLvlHashes[j] = prevLvlHashes[i];
                    }
                }

                prevLvlHashes = currLvlHashes;
            }

            return prevLvlHashes[0];
        }

        public static byte[] CalculateSHA256Hash(SHA256 sha256, byte[] inputBytes, int count)
        {
            sha256.Initialize();

            byte[] hash = sha256.ComputeHash(inputBytes, 0, count);
            return hash;
        }

        private static string CalculateFileSHA256Hash(string file)
        {
            using (FileStream stream = File.OpenRead(file))
            {
                var sha = new SHA256Managed();
                byte[] checksum = sha.ComputeHash(stream);
                return BitConverter.ToString(checksum).Replace("-", String.Empty);
            }
        }
    }
}
