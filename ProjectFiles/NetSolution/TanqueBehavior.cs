#region Using directives
using System;
using UAManagedCore;
using OpcUa = UAManagedCore.OpcUa;
using FTOptix.UI;
using FTOptix.DataLogger;
using FTOptix.HMIProject;
using FTOptix.NetLogic;
using FTOptix.NativeUI;
using FTOptix.SQLiteStore;
using FTOptix.Store;
using FTOptix.OPCUAServer;
using FTOptix.Retentivity;
using FTOptix.CoreBase;
using FTOptix.Core;
#endregion


[CustomBehavior]
public class TanqueBehavior : BaseNetBehavior
{
    public override void Start()
    {
        // Insert code to be executed when the user-defined behavior is started
    }

    public override void Stop()
    {
        // Insert code to be executed when the user-defined behavior is stopped
    }

    [ExportMethod]
    public void LlenarTanque()
    {
        try
        {
            Node.Fill = true;

            if (Node.NivelTanque != 100)
            {
                Node.NivelTanque = 1000;
                Log.Info("Tanque llenado completamente");
            }

        }
        catch (Exception ex)
        {
            Log.Error($"Error al llenar tanque: {ex.Message}");
        }
    }

    [ExportMethod]
    public void VaciarTanque()
    {
        try
        {
            Node.Fill = false;

            if (Node.NivelTanque != 0)
            {
                Node.NivelTanque = 0;
                Log.Info("Tanque vaciado completamente");
            }

        }
        catch (Exception ex)
        {
            Log.Error($"Error al vaciar tanque: {ex.Message}");
        }
    }

#region Auto-generated code, do not edit!
    protected new Tanque Node => (Tanque)base.Node;
#endregion
}
