#region Using directives
using System;
using System.IO;
using System.Text;
using FTOptix.CoreBase;
using FTOptix.HMIProject;
using UAManagedCore;
using OpcUa = UAManagedCore.OpcUa;
using FTOptix.NetLogic;
using FTOptix.EventLogger;
using FTOptix.OPCUAServer;
using FTOptix.UI;
using FTOptix.Store;
using FTOptix.SQLiteStore;
using FTOptix.Core;
using FTOptix.MQTTClient;
#endregion

public class GenericTableExporter : BaseNetLogic
{
    /// <summary>
    /// This method exports data from a generic table to a CSV file.
    /// If no CSV file path is provided, an exception will be thrown.
    /// The method retrieves various parameters such as field delimiter, wrapping fields, table object, store object, and query.
    /// It then executes the query on the store object and writes the result to a CSV file with specified headers and formatting.
    /// If there's an issue executing the query or writing the CSV, an error message will be logged.
    /// </summary>
    [ExportMethod]
    public void Export()
    {
        try
        {
            var csvPath = GetCSVFilePath();
            if (string.IsNullOrEmpty(csvPath))
                throw new Exception("No CSV file chosen, please fill the CSVPath variable");

            char? fieldDelimiter = GetFieldDelimiter();
            bool wrapFields = GetWrapFields();
            var tableObject = GetTable();
            var storeObject = GetStoreObject(tableObject);
            var selectQuery = GetQuery();

            storeObject.Query(selectQuery, out string[] header, out object[,] resultSet);

            if (header == null || resultSet == null)
                throw new Exception("Unable to execute SQL query, malformed result");

            var rowCount = resultSet.GetLength(0);
            var columnCount = resultSet.GetLength(1);

            using (var csvWriter = new CSVFileWriter(csvPath) { FieldDelimiter = fieldDelimiter.Value, WrapFields = wrapFields })
            {
                csvWriter.WriteLine(header);
                WriteTableContent(resultSet, rowCount, columnCount, csvWriter);
            }

            Log.Info("GenericTableExporter", "The table " + tableObject.BrowseName + " has been succesfully exported to " + csvPath);
        }
        catch (Exception ex)
        {
            Log.Error("GenericTableExporter", "Unable to export table: " + ex.Message);
        }
    }

    /// <summary>
    /// Writes table content from a 2D array to a CSV file writer.
    /// <example>
    /// For example:
    /// <code>
    /// object[,] resultSet = { {"Name", "Age", "City"}, {"John Doe", 30, "New York"}, {"Jane Smith", 25, "Los Angeles"} };
    /// int rowCount = 2;
    /// int columnCount = 3;
    /// CSVFileWriter csvWriter = new CSVFileWriter("output.csv");
    /// WriteTableContent(resultSet, rowCount, columnCount, csvWriter);
    /// </code>
    /// The method will write rows with names, ages, and cities into the output.csv file.
    /// </example>
    /// </summary>
    /// <param name="resultSet">A 2D array containing the data to be written.</param>
    /// <param name="rowCount">The number of rows in the dataset.</param>
    /// <param name="columnCount">The number of columns in each row.</param>
    /// <param name="csvWriter">An instance of CSVFileWriter used for writing to a CSV file.</param>
    private void WriteTableContent(object[,] resultSet, int rowCount, int columnCount, CSVFileWriter csvWriter)
    {
        for (var r = 0; r < rowCount; ++r)
        {
            var currentRow = new string[columnCount];

            for (var c = 0; c < columnCount; ++c)
                currentRow[c] = resultSet[r, c]?.ToString() ?? "NULL";

            csvWriter.WriteLine(currentRow);
        }
    }

    /// <summary>
    /// This method retrieves a table from the information model based on its nodeId.
    /// If the table variable is not found or is empty, it throws an exception.
    /// It also validates that the retrieved node is indeed an instance of Table type.
    /// </summary>
    /// <returns>A Table object representing the retrieved table.</returns>
    private Table GetTable()
    {
        var alarmEventLoggerVariable = LogicObject.GetVariable("Table");

        if (alarmEventLoggerVariable == null)
            throw new Exception("Table variable not found");

        NodeId tableNodeId = alarmEventLoggerVariable.Value;
        if (tableNodeId == null || tableNodeId == NodeId.Empty)
            throw new Exception("Table variable is empty");

        var tableNode = InformationModel.Get(tableNodeId) as Table;

        if (tableNode == null)
            throw new Exception("The specified table node is not an instance of Store Table type");

        return tableNode;
    }

    /// <summary>
    /// This method retrieves a Store object from the given Table node's owner.
    /// <example>
    /// For example:
    /// <code>
    /// Store store = GetStoreObject(tableNode);
    /// </code>
    /// will return the Store associated with the provided Table node.
    /// </example>
    /// </summary>
    /// <param name="tableNode">The Table node from which to retrieve the Store object.</param>
    /// <returns>
    /// A Store object representing the store associated with the input Table node.
    /// </returns>
    /// <remarks>
    /// Assumes that the owner of the Table node is an instance of Store.
    /// </remarks>
    private Store GetStoreObject(Table tableNode)
    {
        return tableNode.Owner.Owner as Store;
    }

    /// <summary>
    /// This method retrieves the CSV file path from the logic object's variable 'CSVPath'.
    /// If the 'CSVPath' variable is not found, it throws an exception.
    /// The returned URI represents the full path to the CSV file resource.
    /// </summary>
    /// <returns>
    /// A string representing the URI of the CSV file resource.
    /// </returns>
    private string GetCSVFilePath()
    {
        var csvPathVariable = LogicObject.GetVariable("CSVPath");
        if (csvPathVariable == null)
            throw new Exception("CSVPath variable not found");

        return new ResourceUri(csvPathVariable.Value).Uri;
    }

    /// <summary>
    /// This method retrieves the field delimiter from a logic object's variable named "FieldDelimiter".
    /// It checks for valid configurations by ensuring the variable exists, has non-empty content, length equals one,
    /// and correctly represents a single character. If any condition fails, it throws an exception with an appropriate message.
    /// The method then returns the correct field delimiter as a char.
    /// </summary>
    /// <returns>The field delimiter as a char.</returns>
    private char GetFieldDelimiter()
    {
        var separatorVariable = LogicObject.GetVariable("FieldDelimiter");
        if (separatorVariable == null)
            throw new Exception("FieldDelimiter variable not found");

        string separator = separatorVariable.Value;

        if (separator == String.Empty)
            throw new Exception("FieldDelimiter variable is empty");

        if (separator.Length != 1)
            throw new Exception("Wrong FieldDelimiter configuration. Please insert a single character");

        if (!char.TryParse(separator, out char result))
            throw new Exception("Wrong FieldDelimiter configuration. Please insert a char");

        return result;
    }

    /// <summary>
    /// This method retrieves the value of the "WrapFields" variable from the LogicObject.
    /// If the variable is not found, it throws an exception.
    /// Otherwise, it returns the value of the variable.
    /// </summary>
    /// <returns>The value of the "WrapFields" variable.</returns>
    private bool GetWrapFields()
    {
        var wrapFieldsVariable = LogicObject.GetVariable("WrapFields");
        if (wrapFieldsVariable == null)
            throw new Exception("WrapFields variable not found");

        return wrapFieldsVariable.Value;
    }

    /// <summary>
    /// This method retrieves the query from the logic object and validates it.
    /// If the query variable is not found or is empty/invalid, an exception is thrown.
    /// Otherwise, it returns the query as a string.
    /// </summary>
    /// <returns>A validated query string.</returns>
    private string GetQuery()
    {
        var queryVariable = LogicObject.GetVariable("Query");
        if (queryVariable == null)
            throw new Exception("Query variable not found");

        string query = queryVariable.Value;

        if (String.IsNullOrEmpty(query))
            throw new Exception("Query variable is empty or not valid");

        return query;
    }

    #region CSVFileWriter
    private class CSVFileWriter : IDisposable
    {
        public char FieldDelimiter { get; set; } = ',';

        public char QuoteChar { get; set; } = '"';

        public bool WrapFields { get; set; } = false;

        public CSVFileWriter(string filePath)
        {
            streamWriter = new StreamWriter(filePath, false, System.Text.Encoding.UTF8);
        }

        /// <summary>
        /// Initializes a new instance of the CSVFileWriter class with specified file path and encoding.
        /// <example>
        /// For example:
        /// <code>
        /// CSVFileWriter writer = new CSVFileWriter("path/to/file.csv", System.Text.Encoding.UTF8);
        /// </code>
        /// creates an object that can write to the file at "path/to/file.csv" using UTF-8 encoding.
        /// </example>
        /// </summary>
        /// <param name="filePath">The file path where the CSV data will be written.</param>
        /// <param name="encoding">The character encoding for writing the CSV content.</param>
        /// <returns>
        /// A CSVFileWriter object ready to write CSV data.
        /// </returns>
        /// <remarks>
        /// The constructor initializes a new instance of the StreamWriter class, which is used internally by this class to write CSV formatted text to the specified file.
        /// </remarks>
        public CSVFileWriter(string filePath, System.Text.Encoding encoding)
        {
            streamWriter = new StreamWriter(filePath, false, encoding);
        }

        /// <summary>
        /// Initializes a new instance of the CSVFileWriter class with the provided StreamWriter.
        /// <example>
        /// For example:
        /// <code>
        /// var writer = new CSVFileWriter(new StreamWriter("output.csv"));
        /// </code>
        /// creates a new writer object that writes to "output.csv".
        /// </example>
        /// </summary>
        /// <param name="streamWriter">The StreamWriter used for writing CSV data.</param>
        /// <returns>
        /// A new instance of the CSVFileWriter class configured to write to the specified StreamWriter.
        /// </returns>
        /// <remarks>
        /// The constructor initializes the underlying stream writer with the given parameter.
        /// It prepares the writer for use by setting up its state appropriately.
        /// </remarks>
        public CSVFileWriter(StreamWriter streamWriter)
        {
            this.streamWriter = streamWriter;
        }

        /// <summary>
        /// Writes an array of strings to the specified stream with formatting options applied.
        /// <example>
        /// For example:
        /// <code>
        /// string[] data = {"Name", "Age"};
        /// WriteLine(data);
        /// </code>
        /// will write "Name\"," followed by the age field, then a comma and space.
        /// </example>
        /// </summary>
        /// <param name="fields">Array containing the fields to be written.</param>
        /// <param name="streamWriter">Stream writer object to which the output is appended.</param>
        /// <param name="quoteChar">Character used for quoting fields.</param>
        /// <param name="escapeField">Method that escapes special characters within fields before writing them out.</param>
        /// <param name="wrapFields">Boolean indicating whether to wrap fields with quotes.</param>
        /// <param name="fieldDelimiter">Character separating fields when wrapping is enabled.</param>
        public void WriteLine(string[] fields)
        {
            var stringBuilder = new StringBuilder();

            for (var i = 0; i < fields.Length; ++i)
            {
                if (WrapFields)
                    stringBuilder.AppendFormat("{0}{1}{0}", QuoteChar, EscapeField(fields[i]));
                else
                    stringBuilder.AppendFormat("{0}", fields[i]);

                if (i != fields.Length - 1)
                    stringBuilder.Append(FieldDelimiter);
            }

            streamWriter.WriteLine(stringBuilder.ToString());
            streamWriter.Flush();
        }

        /// <summary>
        /// This method escapes a given field by replacing the quotation character with its double representation.
        /// <example>
        /// For example:
        /// <code>
        /// string escapedField = EscapeField("Hello, 'World'");
        /// </code>
        /// results in <c>escapedField</c>'s having the value "Hello, ''World''".
        /// </example>
        /// </summary>
        /// <param name="field">The field to escape.</param>
        /// <returns>
        /// A string containing the original field with all quotation characters replaced by their double representations.
        /// </returns>
        private string EscapeField(string field)
        {
            var quoteCharString = QuoteChar.ToString();
            return field.Replace(quoteCharString, quoteCharString + quoteCharString);
        }

        private StreamWriter streamWriter;

        #region IDisposable Support
        private bool disposed = false;
        
        protected virtual void Dispose(bool disposing)
        {
            if (disposed)
                return;

            if (disposing)
                streamWriter.Dispose();

            disposed = true;
        }

        /// <summary>
        /// Invokes the finalizer for proper cleanup.
        /// </summary>
        /// <param name="disposing">
        /// A boolean indicating whether the disposing context is true (for manual disposal).
        /// </param>
        public void Dispose()
        {
            Dispose(true);
        }

        #endregion
    }
    #endregion
}
