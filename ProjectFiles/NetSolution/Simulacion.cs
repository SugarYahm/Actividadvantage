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

    // Variables del Model
    private IUAVariable _nivelTanque1;
    private IUAVariable _nivelTanque2;
    private IUAVariable _nivelTanque3;
    private IUAVariable _bombaRun;
    private IUAVariable _caudalDos;
    private IUAVariable _caudalDosActual;
    private IUAVariable _phMezcla;
     // Estado interno del simulador
    private readonly Random _rnd = new Random();

    private double _flowActualLph = 0.0;     // Caudal "real" (L/h) con inercia
    private double _ph = 7.2;                // pH dinámico
    private double _refill = 0.0;            // 0..1 -> indica si está en recarga
    private double _t = 0.0;                 // tiempo para oscilaciones
    public override void Start()
    {
        // Toma variables del Model por path
        _nivelTanque1 = GetVar("Model/NivelTanque1");
        _nivelTanque2 = GetVar("Model/NivelTanque2");
        _nivelTanque3 = GetVar("Model/NivelTanque3");
        _bombaRun     = GetVar("Model/Bomba_Run");
        _caudalDos    = GetVar("Model/CaudalDeDosificación");
        _phMezcla     = GetVar("Model/pH_Mezcla");
        _caudalDosActual = GetVar("Model/CaudalDeDosificación_Actual");


        // Valores iniciales recomendados (no obliga, pero ayuda)
        if (_nivelTanque1.Value == null) _nivelTanque1.Value = 85.0;
        if (_nivelTanque2.Value == null) _nivelTanque2.Value = 70.0;
        if (_nivelTanque3.Value == null) _nivelTanque3.Value = 65.0;
        if (_caudalDos.Value == null) _caudalDos.Value = 25.0; // L/h típico demo
        if (_phMezcla.Value == null) _phMezcla.Value = _ph;
        if (_caudalDosActual.Value == null) _caudalDosActual.Value = 0.0;

        // Ejecuta cada 200 ms
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
        const double dt = 0.2; // 200 ms

        // Lee entradas
        bool pumpOn = Convert.ToBoolean(_bombaRun.Value.Value);
        double flowSet = ToDouble(_caudalDos.Value.Value, 0.0); // L/h (set)
        flowSet = Clamp(flowSet, 0.0, 120.0);

        double levelChem = ToDouble(_nivelTanque1.Value.Value, 80.0); // %
        double levelMix  = ToDouble(_nivelTanque2.Value.Value, 70.0); // %
        double levelRaw  = ToDouble(_nivelTanque3.Value.Value, 65.0); // %

        // --- 1) Lógica de recarga de químico (tanque 1) ---
        // si baja de 10%, activa "refill"; cuando llegue a 90% se apaga
        if (levelChem <= 10.0) _refill = 1.0;
        if (levelChem >= 90.0) _refill = 0.0;

        // recarga sube ~ 6 %/min cuando está activa
        if (_refill > 0.5)
            levelChem += 6.0 * (dt / 60.0);

        // --- 2) Caudal real con inercia (simula bomba + tubería) ---
        // si tanque vacío, limita caudal
        double tankFactor = (levelChem <= 2.0) ? 0.0 : 1.0;

        double target = (pumpOn ? flowSet : 0.0) * tankFactor;

        // Primer orden (lag): flowActual sigue al target suavemente
        const double tau = 1.5; // segundos (respuesta)
        _flowActualLph += (target - _flowActualLph) * (dt / tau);

        // ruido suave ±2%
        _flowActualLph *= (1.0 + (Noise() * 0.02));

        _flowActualLph = Clamp(_flowActualLph, 0.0, 130.0);

        // --- 3) Consumo de químico (tanque 1) ---
        // Supongamos tanque químico de 200 L equivalentes a 100%
        const double tankCapacityL = 200.0;
        double litrosConsumidos = _flowActualLph * (dt / 3600.0);
        double deltaPct = (litrosConsumidos / tankCapacityL) * 100.0;

        if (pumpOn && _refill < 0.5)
            levelChem -= deltaPct;

        levelChem = Clamp(levelChem, 0.0, 100.0);

        // --- 4) Nivel del tanque de mezcla (tanque 2) ---
        // Se mantiene estable, pero sube un poquito si dosificas, y oscila leve
        _t += dt;
        double osc = Math.Sin(_t * 0.5) * 0.15;      // oscilación lenta
        double bump = (_flowActualLph / 120.0) * 0.08; // efecto dosificación
        levelMix += (osc + bump) * 0.5;              // suaviza
        levelMix = Clamp(levelMix, 40.0, 95.0);

        // --- 5) Nivel “agua cruda/proceso” (tanque 3) ---
        // Simula demanda: onda + ruido
        double demandWave = Math.Sin(_t * 0.25) * 0.30;
        levelRaw += demandWave + (Noise() * 0.05);
        levelRaw = Clamp(levelRaw, 35.0, 95.0);

        // --- 6) pH de mezcla ---
        // Base de pH del agua cruda ~7.2
        // El coagulante “ácido” baja pH proporcional al caudal (con saturación y dinámica)
        const double phBase = 7.2;
        double acidEffect = (_flowActualLph / 60.0) * 0.8; // 0..~1.6 aprox
        acidEffect = Clamp(acidEffect, 0.0, 1.8);

        // objetivo de pH baja con dosificación; con bomba off regresa a base
        double phTarget = phBase - acidEffect;

        // dinámica lenta (mezcla): tauPh ~ 8 s
        const double tauPh = 8.0;
        _ph += (phTarget - _ph) * (dt / tauPh);

        // ruido leve
        _ph += Noise() * 0.01;

        _ph = Clamp(_ph, 5.5, 8.5);

        // --- 7) Escribe salidas ---
        _nivelTanque1.Value = levelChem;
        _nivelTanque2.Value = levelMix;
        _nivelTanque3.Value = levelRaw;

        // OJO: tu variable se llama CaudalDeDosificación, pero tú la puedes tratar como setpoint.
        // Si quieres mostrar "caudal real", crea otra variable: CaudalDeDosificación_Actual
        // y aquí se la asignas. Por ahora, dejamos el set tal cual y solo actualizamos pH.
        _phMezcla.Value = _ph;
        _caudalDosActual.Value = _flowActualLph;
    }

    // Helpers
    private IUAVariable GetVar(string path)
    {
        // Busca desde el root del proyecto: "Model/..."
        var v = Project.Current.GetVariable(path);
        if (v == null)
            throw new Exception($"No se encontró la variable: {path}. Revisa que exista en Model.");
        return v;
    }

    private double Noise()
    {
        // -1..+1
        return (_rnd.NextDouble() * 2.0) - 1.0;
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
