#region Using directives
using System;
using UAManagedCore;
using OpcUa = UAManagedCore.OpcUa;
using FTOptix.UI;
using FTOptix.HMIProject;
using FTOptix.NativeUI;
using FTOptix.Retentivity;
using FTOptix.CoreBase;
using FTOptix.Core;
using FTOptix.NetLogic;
using FTOptix.OPCUAServer;
using FTOptix.SQLiteStore;
using FTOptix.Store;
using FTOptix.DataLogger;
#endregion


public class Simulacion : BaseNetLogic
{
    private PeriodicTask _task;

    // Model vars
    private IUAVariable NivelAgua, NivelQuim, NivelMix;
    private IUAVariable BombaRun;
    private IUAVariable pH, pHsp;
    private IUAVariable Qchem, Qwater; // opcional: para mostrar "caudales" 0..100

    // Estado interno
    private double _ph = 7.2;

    // Refill con histéresis
    private bool _refillAgua = false;
    private bool _refillQuim = false;

    public override void Start()
    {
        NivelAgua = GetVar("Model/NivelTanqueAgua");
        NivelQuim = GetVar("Model/NivelTanqueQuimico");
        NivelMix  = GetVar("Model/NivelTanqueMezcla");

        BombaRun = GetVar("Model/Bomba_Run");

        pH   = GetVar("Model/pH_Mezcla");
        pHsp = GetVar("Model/pH_Mezcla_SetPoint");

        Qchem  = GetVar("Model/CaudalDeDosificacion"); // aquí lo usamos como "intensidad 0..100"
        Qwater = GetVar("Model/CaudalDeAgua");         // "intensidad 0..100"

        // Iniciales
        if (NivelAgua.Value == null) NivelAgua.Value = 80.0;
        if (NivelQuim.Value == null) NivelQuim.Value = 70.0;
        if (NivelMix.Value  == null) NivelMix.Value  = 0.0;

        if (pHsp.Value == null) pHsp.Value = 6.9;
        if (pH.Value == null) pH.Value = _ph;

        if (Qwater.Value == null) Qwater.Value = 60.0;
        if (Qchem.Value == null)  Qchem.Value  = 0.0;

        _task = new PeriodicTask(Update, 200, LogicObject);
        _task.Start();
    }

    public override void Stop()
    {
        _task?.Dispose();
        _task = null;
    }

    private void Update()
    {
        const double dt = 0.2; // segundos

        bool on = Convert.ToBoolean(BombaRun.Value.Value);

        double lvlW = Clamp(ToDouble(NivelAgua.Value.Value, 80.0), 0, 100);
        double lvlC = Clamp(ToDouble(NivelQuim.Value.Value, 70.0), 0, 100);
        double lvlM = Clamp(ToDouble(NivelMix.Value.Value, 0.0),  0, 100);

        double sp = Clamp(ToDouble(pHsp.Value.Value, 6.9), 5.5, 8.5);

        // ----------------------------
        // 1) Refill automático
        // ----------------------------
        // Agua
        if (lvlW <= 10.0) _refillAgua = true;
        if (lvlW >= 90.0) _refillAgua = false;

        if (_refillAgua) // refill también puede pasar con bomba OFF (según tu regla?)
        {
            // si quieres que refill ocurra siempre, quita "!on"
            lvlW += 10.0 * (dt); // +10 % por segundo -> visible
        }

        // Químico
        if (lvlC <= 10.0) _refillQuim = true;
        if (lvlC >= 90.0) _refillQuim = false;

        if (_refillQuim && !on)
        {
            lvlC += 18.0 * (dt); // +18 % por segundo
        }

        // ----------------------------
        // 2) Si bomba OFF: nada se mueve, solo pH sube
        // ----------------------------
        if (!on)
        {
            // Congela niveles (pero aplica refill si estaba activo)
            lvlW = Clamp(lvlW, 0, 100);
            lvlC = Clamp(lvlC, 0, 100);
            lvlM = Clamp(lvlM, 0, 100);
            // Vaciar poquito a poquito cuando está OFF (goteo)
            double outMixOff = 0.25 * dt;   // % por segundo aprox (ajustable)
            lvlM -= outMixOff;
            lvlM = Clamp(lvlM, 0, 100);

            // pH sube hacia 7.2 (agua)
            const double phBase = 7.2;
            const double tau = 6.0; // segundos
            _ph += (phBase - _ph) * (dt / tau);
            _ph = Clamp(_ph, 5.5, 8.5);

            // Escribe
            NivelAgua.Value = lvlW;
            NivelQuim.Value = lvlC;
            NivelMix.Value  = lvlM;

            pH.Value = _ph;

            // Caudales visuales en 0 cuando está apagada
            Qwater.Value = 0.0;
            Qchem.Value  = 0.0;
            return;
        }

        // ----------------------------
        // 3) Bomba ON: consumo y mezcla (0..100)
        // ----------------------------
        // Intensidad de agua (fija para demo)
        double waterIntensity = 60.0; // 0..100 (simboliza caudal de agua)
        // Intensidad de químico depende del setpoint:
        // SP más bajo -> más químico.
        // Mapeo: SP 7.2 => 0, SP 6.0 => 80 aprox
        double chemIntensity = Clamp((7.2 - sp) * 65.0, 0.0, 100.0);

        // Guarda “caudales” como intensidades
        Qwater.Value = waterIntensity;
        Qchem.Value  = chemIntensity;

        // Consumo por segundo (visible)
        double dW = (waterIntensity / 100.0) * 2.0 * dt; // hasta -6%/s
        double dC = (chemIntensity  / 100.0) * 1.0 * dt; // hasta -4%/s

        if (!_refillAgua) lvlW -= dW;
        if (!_refillQuim) lvlC -= dC;

        lvlW = Clamp(lvlW, 0, 100);
        lvlC = Clamp(lvlC, 0, 100);

        // Mezcla = suma de aportes 
        double inMix = ((waterIntensity + chemIntensity) / 200.0) * 8.0 * dt; // hasta +8%/s
        lvlM += inMix;
        // --- dinámica para vaciar la mezcla (salida del proceso) ---
        // salida base + salida proporcional a lo lleno que está
        double outMix = (1.2 * dt) + (lvlM / 100.0) * (2.0 * dt);  // % que baja por tick
        lvlM -= outMix;

        // Clamp 0..100
        lvlM = Clamp(lvlM, 0, 100);

        // pH baja con químico e intenta acercarse al SP, pero con dinámica
        double phTarget = 7.2 - (chemIntensity / 100.0) * 1.6; // hasta ~5.6
        phTarget = 0.6 * phTarget + 0.4 * sp;

        const double tauPH = 3.0;
        _ph += (phTarget - _ph) * (dt / tauPH);
        _ph = Clamp(_ph, 5.5, 8.5);

        // ----------------------------
        // 4) Refill 
        // ----------------------------
        if (lvlW <= 10.0) _refillAgua = true;
        if (lvlC <= 10.0) _refillQuim = true;

        // Escribe
        NivelAgua.Value = lvlW;
        NivelQuim.Value = lvlC;
        NivelMix.Value  = lvlM;
        pH.Value = _ph;
    }

    private IUAVariable GetVar(string path)
    {
        var v = Project.Current.GetVariable(path);
        if (v == null) throw new Exception($"No se encontró variable: {path}");
        return v;
    }

    private static double Clamp(double x, double lo, double hi)
    {
        if (x < lo) return lo;
        if (x > hi) return hi;
        return x;
    }

    private static double ToDouble(object v, double fallback)
    {
        if (v == null) return fallback;
        try { return Convert.ToDouble(v); }
        catch { return fallback; }
    }
}