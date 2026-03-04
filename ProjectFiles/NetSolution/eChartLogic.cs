#region Using directives
using System;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using System.Globalization;
using System.Linq;

using UAManagedCore;
using FTOptix.NetLogic;
using FTOptix.HMIProject;
using FTOptix.Store;
#endregion

public class eChartLogic : BaseNetLogic
{
    private HttpListener _listener;
    private Thread _thread;
    private volatile bool _running;

    private IUAVariable _speedVar;
    private double _lastValue;

    // ✅ Carpeta donde están gauge.html, trends.html y echarts.min.js
    private const string Folder =
        @"C:\Users\Axel Ruiz\Documents\Rockwell Automation\FactoryTalk Optix\Projects\Actividadvantage\ProjectFiles\html";

    // ✅ Tabla del DataLogger
    private const string TrendTable = "Logger_eChart_Table";

    // ✅ DataStore embebido
    private const string DataStorePath = "DataStores/DB_eChart";

    public override void Start()
    {
        _speedVar = Project.Current.GetVariable("Model/Speed");
        if (_speedVar != null)
        {
            _speedVar.VariableChange += OnSpeedChanged;
            _lastValue = ToDoubleSafe(_speedVar.Value.Value);
        }

        Log.Info("GaugeHTTP", "GaugeFolder = " + Folder);
        Log.Info("GaugeHTTP", "Expect trends at: " + Path.Combine(Folder, "trends.html"));

        _listener = new HttpListener();
        _listener.Prefixes.Add("http://127.0.0.1:8088/");
        _listener.Start();

        _running = true;
        _thread = new Thread(ServerLoop) { IsBackground = true };
        _thread.Start();

        Log.Info("GaugeHTTP", "Try: http://127.0.0.1:8088/trends.html");
    }

    public override void Stop()
    {
        if (_speedVar != null)
            _speedVar.VariableChange -= OnSpeedChanged;

        _running = false;
        try { _listener?.Stop(); } catch { }
        try { _listener?.Close(); } catch { }
    }

    private void OnSpeedChanged(object sender, VariableChangeEventArgs e)
    {
        _lastValue = ToDoubleSafe(e.NewValue.Value);
    }

    private void ServerLoop()
    {
        while (_running && _listener != null && _listener.IsListening)
        {
            try
            {
                var ctx = _listener.GetContext();
                Handle(ctx);
            }
            catch
            {
                if (!_running) break;
            }
        }
    }

    private void Handle(HttpListenerContext ctx)
    {
        var req = ctx.Request;
        var res = ctx.Response;

        res.AddHeader("Access-Control-Allow-Origin", "*");

        string path = req.Url.AbsolutePath;

        // -------------------------
        // JSON: Gauge
        // -------------------------
        if (path.Equals("/value", StringComparison.OrdinalIgnoreCase))
        {
            string json = "{ \"value\": " + _lastValue.ToString(CultureInfo.InvariantCulture) + " }";
            WriteText(res, "application/json; charset=utf-8", json);
            return;
        }


        // -------------------------
        // HTML
        // -------------------------

        if (path.Equals("/trends.html", StringComparison.OrdinalIgnoreCase))
        {
            WriteFile(res, "text/html; charset=utf-8", Path.Combine(Folder, "trends.html"));
            return;
        }

        // -------------------------
        // JS libs
        // -------------------------
        if (path.Equals("/echarts.min.js", StringComparison.OrdinalIgnoreCase))
        {
            WriteFile(res, "application/javascript; charset=utf-8", Path.Combine(Folder, "echarts.min.js"));
            return;
        }

        if (path.Equals("/api/trends", StringComparison.OrdinalIgnoreCase))
        {
            int windowSec = Clamp(ParseInt(req.QueryString["windowSec"], 60), 1, 24 * 3600);
            int points    = Clamp(ParseInt(req.QueryString["points"], 60), 2, 2000);

            DateTime? from = ParseDate(req.QueryString["from"]);
            DateTime? to   = ParseDate(req.QueryString["to"]);

            try
            {
                string json = BuildTrendsJson(windowSec, points, from, to);
                WriteText(res, "application/json; charset=utf-8", json);
            }
            catch (Exception ex)
            {
                res.StatusCode = 500;
                WriteText(res, "application/json; charset=utf-8",
                    "{ \"error\": " + JsonEscape(ex.Message) + " }");
            }
            return;
        }

        // 404
        res.StatusCode = 404;
        WriteText(res, "text/plain; charset=utf-8", "404 " + path);
        return;
    }

    private string PingDb()
    {
        var store = Project.Current.Get<Store>(DataStorePath);
        if (store == null) throw new Exception("Store not found");

        string sql = "SELECT 'ok'"; // SIN números
        Log.Info("SQL", sql);

        string[] header;
        object[,] rows;
        store.Query(sql, out header, out rows);

        string h = (header != null && header.Length > 0) ? header[0] : "(no header)";
        string v = (rows.GetLength(0) > 0 && rows.GetLength(1) > 0 && rows[0,0] != null) ? rows[0,0].ToString() : "(no rows)";

        return System.Text.Json.JsonSerializer.Serialize(new { sql, header = h, value = v });
    }

    // ============================================
    // Trends desde Store (DINÁMICO) - SIN NÚMEROS EN SQL
    // ============================================
    private string BuildTrendsJson(int windowSec, int points, DateTime? from, DateTime? to)
    {
        var store = Project.Current.Get<Store>(DataStorePath);
        if (store == null)
            throw new Exception($"No se encontró el DataStore en '{DataStorePath}'");

        var cols = GetColumns_NoNumbers(store, TrendTable);
        if (cols.Count == 0)
            throw new Exception($"No hay columnas en '{TrendTable}' (¿ya está escribiendo el logger?)");

        var timeCol = DetectTimeColumn(cols);
        var vars = DetectLoggedVariables(cols, timeCol);
        if (vars.Count == 0)
            throw new Exception($"No se detectaron variables en '{TrendTable}'");

        string select = QuoteIdent(timeCol) + ", " + string.Join(", ", vars.Select(QuoteIdent));
        string sql = $"SELECT {select} FROM {QuoteIdent(TrendTable)} ORDER BY {QuoteIdent(timeCol)} DESC";

        string[] header;
        object[,] rows;

        Log.Info("SQL", sql);
        store.Query(sql, out header, out rows);

        var now = DateTime.Now;

        DateTime minTime = from ?? now.AddSeconds(-windowSec);
        DateTime maxTime = to   ?? now;

        if (maxTime < minTime) { var tmp = minTime; minTime = maxTime; maxTime = tmp; }

        var timesDesc = new System.Collections.Generic.List<string>();
        var seriesDesc = new System.Collections.Generic.Dictionary<string, System.Collections.Generic.List<object>>();
        foreach (var v in vars) seriesDesc[v] = new System.Collections.Generic.List<object>();

        int rowCount = rows.GetLength(0);

        for (int r = 0; r < rowCount; r++) // DESC: más nuevo primero
        {
            var tsRaw = rows[r, 0]?.ToString() ?? "";

            if (!TryParseTimestamp(tsRaw, out var ts))
                ts = now;

            // Como viene DESC: si ya bajamos de minTime, ya no habrá más dentro del rango
            if (ts < minTime) break;

            // Si está por arriba del rango (futuro o mayor que "to"), lo saltas
            if (ts > maxTime) continue;

            timesDesc.Add(FormatTime(tsRaw));

            for (int i = 0; i < vars.Count; i++)
                seriesDesc[vars[i]].Add(rows[r, 1 + i]);
        }

        if (timesDesc.Count == 0)
        {
            var emptySource = new object[][]
            {
                new object[] { "tag" }
            };
            return System.Text.Json.JsonSerializer.Serialize(new { source = emptySource });
        }

        timesDesc.Reverse();
        foreach (var v in vars) seriesDesc[v].Reverse();

        var source = new System.Collections.Generic.List<System.Collections.Generic.List<object>>();

        var headerRow = new System.Collections.Generic.List<object> { "tag" };
        headerRow.AddRange(timesDesc.Cast<object>());
        source.Add(headerRow);

        foreach (var v in vars)
        {
            var row = new System.Collections.Generic.List<object> { v };
            row.AddRange(seriesDesc[v]);
            source.Add(row);
        }

        return System.Text.Json.JsonSerializer.Serialize(new { source });
    }

    private static int Clamp(int v, int min, int max)
    {
        if (v < min) return min;
        if (v > max) return max;
        return v;
    }

    private static DateTime? ParseDate(string s)
    {
        if (string.IsNullOrWhiteSpace(s)) return null;

        s = s.Trim();

        // A veces llega con espacio en lugar de 'T'
        // ej: 2026-03-03 13:30:58
        if (s.Length >= 19 && s[10] == ' ') s = s.Substring(0,10) + "T" + s.Substring(11);

        // Intento EXACTO en formatos típicos (recomendado)
        string[] fmts = new[]
        {
            "yyyy-MM-dd'T'HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm",
            "yyyy-MM-dd'T'HH:mm:ss.fff",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd HH:mm"
        };

        if (DateTime.TryParseExact(
                s,
                fmts,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeLocal | DateTimeStyles.AllowWhiteSpaces,
                out var dtExact))
            return dtExact;

        // Fallback (por si llega algo raro)
        if (DateTime.TryParse(
                s,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeLocal | DateTimeStyles.AllowWhiteSpaces,
                out var dt))
            return dt;

        if (DateTime.TryParse(
                s,
                CultureInfo.CurrentCulture,
                DateTimeStyles.AssumeLocal | DateTimeStyles.AllowWhiteSpaces,
                out dt))
            return dt;

        return null;
    }

    private static bool TryParseTimestamp(string tsRaw, out DateTime ts)
    {
        if (DateTime.TryParse(tsRaw, CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeLocal | DateTimeStyles.AllowWhiteSpaces, out ts))
            return true;

        if (DateTime.TryParse(tsRaw, CultureInfo.CurrentCulture,
            DateTimeStyles.AssumeLocal | DateTimeStyles.AllowWhiteSpaces, out ts))
            return true;

        ts = default;
        return false;
    }

    // Obtener columnas SIN usar WHERE 1=0 / LIMIT 0 / etc.
    private static System.Collections.Generic.List<string> GetColumns_NoNumbers(Store store, string tableName)
    {
        string[] header;
        object[,] rows;

        string sql = $"SELECT * FROM {QuoteIdent(tableName)}";
        Log.Info("SQL", sql);
        store.Query(sql, out header, out rows);

        if (header == null || header.Length == 0)
            return new System.Collections.Generic.List<string>();

        return header.ToList();
    }

    private static string DetectTimeColumn(System.Collections.Generic.List<string> cols)
    {
        // candidatos típicos de Optix
        string[] candidates = { "LocalTimestamp", "Timestamp", "TimeStamp", "Time", "DateTime" };
        foreach (var c in candidates)
        {
            foreach (var x in cols)
            {
                if (x.Equals(c, StringComparison.OrdinalIgnoreCase))
                    return x;
            }
        }
        // fallback: primera columna
        return cols[0];
    }

    private static System.Collections.Generic.List<string> DetectLoggedVariables(System.Collections.Generic.List<string> cols, string timeCol)
    {
        return cols
            .Where(c =>
                !c.Equals(timeCol, StringComparison.OrdinalIgnoreCase) &&
                !c.Equals("OperationCode", StringComparison.OrdinalIgnoreCase) &&
                !c.Equals("Id", StringComparison.OrdinalIgnoreCase) &&
                !c.Equals("Timestamp", StringComparison.OrdinalIgnoreCase) &&
                !c.Equals("Temp_Timestamp", StringComparison.OrdinalIgnoreCase) &&
                !c.Equals("Speed_Timestamp", StringComparison.OrdinalIgnoreCase) &&
                !c.Equals("Flow_Timestamp", StringComparison.OrdinalIgnoreCase) &&
                !c.Equals("Pressure_Timestamp", StringComparison.OrdinalIgnoreCase) &&
                !c.Equals("OperationCodeOfEachVariable", StringComparison.OrdinalIgnoreCase)
            )
            .ToList();
    }

    // ============================================
    // Helpers
    // ============================================
    private static int ParseInt(string s, int fallback)
        => int.TryParse(s, out var v) ? v : fallback;

    private static string QuoteIdent(string ident)
    {
        // Soporta nombres con espacios y evita el parser raro
        return "\"" + (ident ?? "").Replace("\"", "\"\"") + "\"";
    }
    private static string FormatTime(string ts)
    {
        if (ts.Length >= 19) return ts.Substring(11, 8);
        return ts;
    }

    private static string JsonEscape(string s)
        => "\"" + (s ?? "").Replace("\\", "\\\\").Replace("\"", "\\\"") + "\"";

    private static void WriteText(HttpListenerResponse res, string contentType, string text)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(text);
        if (res.StatusCode == 0) res.StatusCode = 200;
        res.ContentType = contentType;
        res.ContentLength64 = bytes.Length;
        res.OutputStream.Write(bytes, 0, bytes.Length);
        res.OutputStream.Close();
    }

    private static void WriteFile(HttpListenerResponse res, string contentType, string filePath)
    {
        if (!File.Exists(filePath))
        {
            res.StatusCode = 404;
            WriteText(res, "text/plain; charset=utf-8", "Not found: " + filePath);
            return;
        }

        byte[] fileBytes = File.ReadAllBytes(filePath);
        res.StatusCode = 200;
        res.ContentType = contentType;
        res.ContentLength64 = fileBytes.Length;
        res.OutputStream.Write(fileBytes, 0, fileBytes.Length);
        res.OutputStream.Close();
    }

    private static double ToDoubleSafe(object o)
    {
        if (o == null) return 0;
        if (o is double d) return d;
        if (o is float f) return f;
        if (o is int i) return i;
        if (o is long l) return l;
        if (o is string s && double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var dv))
            return dv;
        return Convert.ToDouble(o, CultureInfo.InvariantCulture);
    }
}