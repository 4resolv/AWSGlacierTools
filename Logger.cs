using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Threading;

namespace GlacierTools
{
    public class Logger
    {
        static string logDir = null;

        public static void LogMessage(string message)
        {
            if (logDir == null)
            {
                string newPath = Path.Combine(@"c:\ProgramData\GlacierTools");
                Directory.CreateDirectory(newPath);
                logDir = newPath;
            }

            string logPath = Path.Combine(logDir, DateTime.Now.ToString("yyyy-MM-dd_HH"));

            Console.WriteLine(message);

            string logLine = String.Format("{0} [{1:00000}:{2:00000}]: {3}",
                DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture),
                Process.GetCurrentProcess().Id, Thread.CurrentThread.ManagedThreadId, message);

            int attempt = 0;
            while (attempt < 5)
            {
                try
                {
                    using (StreamWriter writer = File.AppendText(logPath))
                    {
                        writer.WriteLine(logLine);
                    }
                    break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                    Thread.Sleep(50);
                }
            }
        }
    }
}
