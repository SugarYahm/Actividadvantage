#region Using directives
using System;
using System.IO;
using FTOptix.Core;
using UAManagedCore;
using FTOptix.NetLogic;
using FTOptix.Store;
using FTOptix.HMIProject;
using FTOptix.CoreBase;   // MethodInvocation
using FTOptix.MQTTClient;
#endregion

public class SchedulerLogic : BaseNetLogic
{
    private PeriodicTask periodicTask;
    private Store schedulerDB;
    private IUAVariable forceCheck;

    private MethodInvocation exportAction; // SchedulerLogic/ExportAction

    private int lastMinute = 0;

    public override void Start()
    {
        lastMinute = DateTime.Now.Minute;

        // Store Scheduler (ruta explícita según tu árbol)
        schedulerDB = Project.Current.Get<Store>("DataStores/SchedulerDB");
        if (schedulerDB == null)
            throw new CoreConfigurationException("No se encontró DataStores/SchedulerDB");

        // MethodInvocation configurado para apuntar a NetLogic/GenericTableExporter/Export
        exportAction = LogicObject.Get("ExportAction") as MethodInvocation;
        if (exportAction == null)
            throw new CoreConfigurationException("No se encontró el MethodInvocation 'ExportAction' dentro de SchedulerLogic");

        // Variables
        forceCheck = LogicObject.GetVariable("ForceCheck");
        if (forceCheck == null)
            throw new CoreConfigurationException("Variable ForceCheck no encontrada en SchedulerLogic");

        forceCheck.VariableChange += ForceCheck_VariableChange;

        periodicTask = new PeriodicTask(CheckChangedMinute, 1000, LogicObject);
        periodicTask.Start();

        CheckScheduler();
    }

    public override void Stop()
    {
        if (forceCheck != null)
            forceCheck.VariableChange -= ForceCheck_VariableChange;

        periodicTask?.Dispose();
        periodicTask = null;
    }

    private void ForceCheck_VariableChange(object sender, VariableChangeEventArgs e)
    {
        if ((bool)forceCheck.Value)
        {
            CheckScheduler();
            forceCheck.Value = false;
        }
    }

    private void CheckChangedMinute()
    {
        if (DateTime.Now.Minute != lastMinute)
        {
            CheckScheduler();
            lastMinute = DateTime.Now.Minute;
        }
    }

    /// <summary>
    /// Evalúa el scheduler cada cambio de minuto.
    /// EXPORTA 1 VEZ POR CADA BLOQUE DE 15 MIN ACTIVADO (aunque sean contiguos).
    /// </summary>
    private void CheckScheduler()
    {
        // AutoEnabled (si no existe, se asume True)
        var autoVar = LogicObject.GetVariable("AutoEnabled");
        if (autoVar != null && !(bool)autoVar.Value)
        {
            SetActive(false);
            return;
        }

        int day = (int)DateTime.Now.DayOfWeek;           // 0=Dom..6=Sáb
        int minNow = DateTime.Now.Hour * 60 + DateTime.Now.Minute;
        int quarter = minNow / 15;                       // 0..95

        bool activeNow = IsQuarterActive(day, quarter);
        SetActive(activeNow);

        // NUEVO: exporta 1 vez por quarter activo (sin depender de flanco)
        if (activeNow)
        {
            // key único por día+quarter (usa DayOfYear para evitar repetir en distintos días)
            int key = (DateTime.Now.DayOfYear * 1000) + (day * 100) + quarter;

            var lastKeyVar = LogicObject.GetVariable("LastFiredKey"); // Int32 recomendado
            int lastKey = lastKeyVar != null ? (int)lastKeyVar.Value : -1;

            if (key != lastKey)
            {
                PrepareCsvPath();       // setea CSVPath con timestamp
                exportAction.Invoke();  // llama a GenericTableExporter.Export

                if (lastKeyVar != null)
                    lastKeyVar.Value = key;
            }
        }
    }

    private void SetActive(bool state)
    {
        var activeVar = LogicObject.GetVariable("Active");
        if (activeVar != null)
            activeVar.Value = state;
    }

    // Scheduler de 15 minutos: DayX tiene 96 chars (1 por quarter)
    private bool IsQuarterActive(int day, int quarter)
    {
        schedulerDB.Query($"SELECT Day{day} FROM Scheduler", out _, out var resultSet);
        if (resultSet.GetLength(0) == 0)
            return false;

        string map = resultSet[0, 0]?.ToString() ?? "";
        if (map.Length <= quarter)
            return false;

        return map[quarter] == '1';
    }

    /// <summary>
    /// (Opcional) Export manual desde método, si lo quieres usar.
    /// </summary>
    [ExportMethod]
    public void ExportNow()
    {
        PrepareCsvPath();
        exportAction.Invoke();
    }

    /// <summary>
    /// Genera un CSVPath único y lo asigna al NetLogic/GenericTableExporter (propiedad CSVPath).
    /// Requiere variables opcionales: ExportFolder, FilePrefix en este SchedulerLogic.
    /// </summary>
    private void PrepareCsvPath()
    {
        // Variables opcionales (si no existen usa defaults)
        string folder = (string)(LogicObject.GetVariable("ExportFolder")?.Value ?? "Exports");
        string prefix = (string)(LogicObject.GetVariable("FilePrefix")?.Value ?? "Calderas");

        // ApplicationDirectory del runtime/emulator
        string outDir = Path.Combine(Project.Current.ApplicationDirectory, folder);
        Directory.CreateDirectory(outDir);

        // nombre único
        string stamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
        string fullPath = Path.Combine(outDir, $"{prefix}_{stamp}.csv");

        // Asigna al GenericTableExporter.CSVPath (AbsoluteResourceUri)
        var exporterObj = Project.Current.GetObject("NetLogic/GenericTableExporter");
        if (exporterObj == null)
            throw new CoreConfigurationException("No se encontró NetLogic/GenericTableExporter");

        var csvPathVar = exporterObj.GetVariable("CSVPath");
        if (csvPathVar == null)
            throw new CoreConfigurationException("GenericTableExporter no tiene variable CSVPath");

        csvPathVar.Value = ResourceUri.FromAbsoluteFilePath(fullPath);
    }
}
