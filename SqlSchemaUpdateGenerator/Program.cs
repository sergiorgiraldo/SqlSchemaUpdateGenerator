using System;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using DBDiff.Schema;
using DBDiff.Schema.SQLServer.Generates.Generates;
using DBDiff.Schema.SQLServer.Generates.Options;
using Microsoft.SqlServer.Management.Smo;
using Column = DBDiff.Schema.SQLServer.Generates.Model.Column;
using Table = DBDiff.Schema.SQLServer.Generates.Model.Table;

//After an idea from Harry McIntyre
namespace SqlSchemaUpdateGenerator
{
    public class Program
    {
        private static string _sourceServer;
        private static string _sourceDb;
        private static string _destServer;
        private static string _destDb;
        
        internal static string ScriptOutputDirectory;
        internal static bool LogToFile = false;

        private static void Main(string[] args)
        {
            var p = new OptionSet
                {
                    {"sourceServer=|sourceSrv=|srcServer=|srcSrv=|originServer=|originSrv=", v => _sourceServer = v},
                    {"destinationServer=|destinationSrv=|destServer=|destSrv=", v => _destServer = v},

                    {
                        "sourceDb=|srcDb=|originDb=|sourceDatabase=|srcDatabase=|originDatabase="
                        , v => _sourceDb = v
                    },

                    {
                        "destinationDb=|destDb=|destinationDatabase=|destDatabase="
                        , v => _destDb = v
                    },

                    {
                        "output=|out=|scriptOutput=|scriptOutputDirectory=|scriptOutputDir=|OutputDir=",
                        v => ScriptOutputDirectory = v
                    },
                    {"log|logToFile", v => LogToFile = true},
                    {"?|help|ajuda", v => Help()}
                };

            p.Parse(args);

            try
            {
                Validate();

                GenerateScript();
            }
            catch (Exception e)
            {
                Log.WriteError(e.Message + Environment.NewLine + e.StackTrace);
            }
        }

        private static void Help()
        {
            string help = typeof (Program).Assembly.FullName + " " + Environment.NewLine;
            help += "\t{sourceServer|sourceSrv|srcServer|srcSrv|originServer|originSrv} : source server" +
                    Environment.NewLine;
            help += "\t{destinationServer|destinationSrv|destServer|destSrv} : destination server" + Environment.NewLine;
            help +=
                "\t{sourceDb|sourceDb|srcDb|srcDb|originDb|originDb|sourceDatabase|srcDatabase|originDatabase} : source database" +
                Environment.NewLine;
            help +=
                "\t[destinationDb|destinationDb|destDb|destDb|destinationDatabase|destDatabase]  : destination database. if ommited, use same name from source." +
                Environment.NewLine;
            help +=
                "\t[output|out|scriptOutput|scriptOutputDirectory|scriptOutputDir|OutputDir]  : destination database. if ommited, use same folder from executable." +
                Environment.NewLine;
            help += "\t[log|logToFile]  : if ommited, only output log to console." + Environment.NewLine;
            help += "\t[?|help|ajuda] : this help";
            Console.WriteLine(help);
        }

        private static void Validate()
        {
            if (string.IsNullOrEmpty(_sourceServer) ||
                string.IsNullOrEmpty(_destServer) ||
                string.IsNullOrEmpty(_sourceDb))
                throw new ArgumentException("Must specify at least SourceServer, Destination Server and Source Database");

            if (string.IsNullOrEmpty(_destDb))
                _destDb = _sourceDb;

            if (string.IsNullOrEmpty(ScriptOutputDirectory))
                ScriptOutputDirectory = Path.GetDirectoryName(typeof (Program).Assembly.Location);
        }

        public static void GenerateScript()
        {
            Log.WriteInfo("Starting " + typeof(Program).Assembly.GetName().Name + " at " + DateTime.Now);
            Log.WriteInfo("sourceServer::" + _sourceServer);
            Log.WriteInfo("sourceDb::" + _sourceDb);
            Log.WriteInfo("destinationServer::" + _destServer);
            Log.WriteInfo("destinationDb::" + _destDb);
            Log.WriteInfo("output" + ScriptOutputDirectory);

            var source =
                new SqlConnectionStringBuilder("server=" + _sourceServer + ";database=" + _sourceDb +
                                               ";trusted_connection=true");
            var destination = new SqlConnectionStringBuilder("server=" + _destServer + ";database=" + _destDb +
                                                             ";trusted_connection=true");
            var sw = new Stopwatch();
            sw.Start();
            var oldSmoServer = new Server(source.DataSource);
            Database oldSmoDb = oldSmoServer.Databases[source.InitialCatalog];
            if (oldSmoDb == null)
            {
                oldSmoDb = new Database(oldSmoServer, source.InitialCatalog);
                oldSmoDb.Create();
            }

            var sql = new Generate
                {
                    ConnectionString = source.ToString(),
                    Options = new SqlOption()
                };

            DBDiff.Schema.SQLServer.Generates.Model.Database sourceDatabase = sql.Process();

            sql.ConnectionString = destination.ToString();
            DBDiff.Schema.SQLServer.Generates.Model.Database destinationDatabase = sql.Process();

            DBDiff.Schema.SQLServer.Generates.Model.Database diff = Generate.Compare(destinationDatabase, sourceDatabase);
            var script = new StringBuilder();
            bool issues = false;

            foreach (Table droppedTable in diff.Tables
                                               .Where(t => t.Status == Enums.ObjectStatusType.DropStatus))
            {
                Log.WriteError("Table drop: " + droppedTable.Name);
                script.AppendLine(droppedTable.ToSqlDrop());
                issues = true;
            }

            foreach (Table table in diff.Tables)
            {
                foreach (Column droppedColumn in table.Columns
                                                      .Where(c => c.Status == Enums.ObjectStatusType.DropStatus))
                {
                    Column renamedColumn =
                        table.Columns.SingleOrDefault(c => c.Id == droppedColumn.Id && c.Name != droppedColumn.Name);
                    if (renamedColumn != null)
                    {
                        Log.WriteError("Column rename: " + droppedColumn.Parent + "." + droppedColumn.Name);
                        Func<string, string> addSquareBracketsIfContainsADot =
                            s => s.Contains(".") ? ("[" + s + "]") : s;
                        script.AppendFormat("\r\nsp_rename '{0}.{1}', '{2}', 'COLUMN';\r\nGO",
                                            addSquareBracketsIfContainsADot(droppedColumn.Parent.Name),
                                            addSquareBracketsIfContainsADot(droppedColumn.Name),
                                            renamedColumn.Name);
                    }
                    else
                    {
                        Log.WriteError("Column drop: " + droppedColumn.Parent.Name + "." + droppedColumn.Name);
                        script.AppendLine(droppedColumn.ToSqlDrop());
                    }
                    issues = true;
                }
            }


            if (issues == false)
            {
                string updateScript = diff.ToSqlDiff().ToSQL();
                updateScript =
                    updateScript.Substring(updateScript.IndexOf("GO", StringComparison.InvariantCulture) + 2).Trim();
                script.AppendLine(updateScript);
                string contents = script.ToString().Trim();
                if (contents.Length > 0)
                {
                    WriteScriptToDisk(contents);
                }
            }
            else
            {
                Log.WriteWarning("Script not saved due to possible loss of data. Check errors");
            }
            sw.Stop();
            Log.WriteInfo("Finished " + typeof (Program).Assembly.GetName().Name + " " + sw.ElapsedMilliseconds +
                          "ms");
        }

        private static void WriteScriptToDisk(string scriptContent)
        {
            string scriptNameBeginnning = _sourceDb + "_v_";
            const string scriptNameEnding = ".sql";

            var directory = new DirectoryInfo(ScriptOutputDirectory);
            var versionNumber = 0;
            var scriptVersions = directory.GetFiles("*" + scriptNameEnding)
                                          .Where(fileInfo => fileInfo.Name.StartsWith(scriptNameBeginnning) &&
                                                             int.TryParse(
                                                                 fileInfo.Name.Replace(scriptNameBeginnning, "")
                                                                         .Replace(scriptNameEnding, ""),
                                                                 out versionNumber))
                                          .Select(fileInfo => new {Version = versionNumber});
            var highest = scriptVersions.OrderByDescending(script => script.Version).FirstOrDefault();
            int version = highest != null ? highest.Version + 1 : 1;
            string scriptFileName = string.Format("{0}{1}{2}", scriptNameBeginnning,
                                                  version.ToString(CultureInfo.InvariantCulture).PadLeft(5, '0'),
                                                  scriptNameEnding);
            Log.WriteWarning("Created script: " + scriptFileName);

            scriptContent =
                string.Format(
                    "/*{0}" +
                    "//script generated at {1}{0}" +
                    "//script generated by {2}{0}" +
                    "//script generated in {3}{0}" +
                    "*/{0}" +
                    "{4}",
                    Environment.NewLine, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), Environment.UserName, Environment.MachineName, scriptContent);

            File.WriteAllText(Path.Combine(directory.FullName, scriptFileName), scriptContent);
        }
    }
}