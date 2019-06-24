using System;
using System.IO;

namespace SqlSchemaUpdateGenerator
{
    internal class Log
    {
        internal static void WriteInfo(string what)
        {
            WriteMessage(typeof (Program).Assembly.GetName().Name, Category.Info, what);
        }

        internal static void WriteWarning(string what)
        {
            WriteMessage(typeof(Program).Assembly.GetName().Name, Category.Warning, what);
        }

        internal static void WriteError(string what)
        {
            WriteMessage(typeof (Program).Assembly.GetName().Name, Category.Error, what);
        }

        private static void WriteMessage(string origin, Category category, string text)
        {
            string message = string.Format("{0} : {1} : {2}", origin, category, text);

            if (Program.LogToFile)
            {
                string fileName = Path.Combine(Program.ScriptOutputDirectory, typeof (Program).Assembly.GetName().Name + "." +
                                  DateTime.Now.ToString("yyyyMMddHHmmss") +
                                  ".log");
                File.AppendAllText(fileName, message);
            }
            else
            {
                Console.WriteLine(message);
            }
        }

        private enum Category
        {
            Info,
            Error,
            Warning
        }
    }
}