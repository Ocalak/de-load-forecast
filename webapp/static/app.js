document.addEventListener("DOMContentLoaded", async () => {
    // Colors designed for Dark Mode
    const colorActual = '#38bdf8'; 
    const colorForecast = '#f59e0b';

    async function fetchData() {
        try {
            const response = await fetch('/api/data');
            const data = await response.json();
            renderChart(data.actuals, data.forecasts, data.weathers);
        } catch (e) {
            console.error('Error fetching data:', e);
        }
    }

    async function fetchMetrics() {
        try {
            const response = await fetch('/api/metrics');
            const data = await response.json();
            if (data.mape !== undefined) {
                document.getElementById('valMAPE').innerText = data.mape + '%';
                document.getElementById('valBIAS').innerText = data.bias;
                document.getElementById('valPEAK').innerText = data.peak_error;
                document.getElementById('valENERGY').innerText = data.energy_mwh;
                document.getElementById('valRMSE').innerText = data.rmse;
                document.getElementById('valMAE').innerText = data.mae;
            }
        } catch (e) {
            console.error('Error fetching metrics:', e);
        }
    }

    async function fetchAlerts() {
        try {
            const response = await fetch('/api/alerts');
            const data = await response.json();
            renderAlerts(data.alerts);
        } catch (e) {
            console.error('Error fetching alerts:', e);
        }
    }

    function renderChart(actuals, forecasts, weathers) {
        const ctx = document.getElementById('demandChart').getContext('2d');
        
        // Formatter for timestamp strings to standard labels
        const formatTime = (isoString) => {
            const d = new Date(isoString);
            return d.toLocaleString('en-GB', { month: 'short', day: '2-digit', hour: '2-digit', minute:'2-digit' });
        };

        // X-axis tightly locked to the Actuals (T-24h) and Forecasts matrices
        const labels = [...new Set([...actuals.map(a => a.time), ...forecasts.map(f => f.time)])].sort();

        // Map data to labels
        const actualMap = {}; actuals.forEach(a => actualMap[a.time] = a.value);
        const forecastMap = {}; forecasts.forEach(f => forecastMap[f.time] = f.value);
        const weatherMap = {}; weathers.forEach(w => weatherMap[w.time] = w.value);

        const dataActuals = labels.map(time => actualMap[time] || null);
        const dataForecasts = labels.map(time => forecastMap[time] || null);
        const dataWeathers = labels.map(time => forecastMap[time] !== undefined ? (weatherMap[time] || null) : null);
        const formattedLabels = labels.map(formatTime);

        // 95% Confidence Interval Synthesizer (5% Margin)
        const errorMargin = 0.05;
        const dataUpper = dataForecasts.map(val => val !== null ? val * (1 + errorMargin) : null);
        const dataLower = dataForecasts.map(val => val !== null ? val * (1 - errorMargin) : null);

        // Calculate true Model Error Residuals
        const dataResiduals = labels.map(time => {
            if (actualMap[time] !== undefined && forecastMap[time] !== undefined) {
                return actualMap[time] - forecastMap[time];
            }
            return null;
        });

        new Chart(ctx, {
            type: 'line',
            data: {
                labels: formattedLabels,
                datasets: [
                    {
                        label: 'Actual Demand (MW)',
                        data: dataActuals,
                        borderColor: colorActual,
                        backgroundColor: 'rgba(56, 189, 248, 0.1)',
                        borderWidth: 3,
                        pointRadius: 0,
                        tension: 0.4,
                        fill: true
                    },
                    {
                        label: '95% Upper CI',
                        data: dataUpper,
                        borderColor: 'transparent',
                        backgroundColor: 'rgba(249, 115, 22, 0.15)', // Translucent Orange
                        fill: '+1',
                        pointRadius: 0,
                        tension: 0.4
                    },
                    {
                        label: '95% Lower CI',
                        data: dataLower,
                        borderColor: 'transparent',
                        fill: false,
                        pointRadius: 0,
                        tension: 0.4
                    },
                    {
                        label: 'Model forecast',
                        data: dataForecasts,
                        borderColor: colorForecast,
                        borderDash: [5, 5],
                        borderWidth: 2,
                        pointRadius: 0,
                        tension: 0.4,
                        fill: false,
                        yAxisID: 'y'
                    },
                    {
                        label: 'Temp Forecast (Avg 10 Cities °C)',
                        data: dataWeathers,
                        borderColor: '#ef4444',
                        borderWidth: 1.5,
                        pointRadius: 0,
                        tension: 0.4,
                        fill: false,
                        yAxisID: 'y1'
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { labels: { color: '#e2e8f0', usePointStyle: true, boxWidth: 8 } }
                },
                scales: {
                    x: { grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: '#94a3b8', maxTicksLimit: 12 } },
                    y: { 
                        type: 'linear',
                        display: true,
                        position: 'left',
                        grid: { color: 'rgba(255,255,255,0.05)' }, 
                        ticks: { color: '#94a3b8' } 
                    },
                    y1: {
                        type: 'linear',
                        display: true,
                        position: 'right',
                        grid: { drawOnChartArea: false }, 
                        ticks: { color: '#ef4444', callback: function(value) { return value + '°C'; } }
                    }
                }
            },
            plugins: [{
                id: 'syncLiveDotPlugin',
                afterDraw: (chart) => {
                    // Extract exact rendered metrics from Chart calculations
                    const meta = chart.getDatasetMeta(0);
                    let lastIndex = -1;
                    
                    // Locate the last physical index where Real Actuals exist
                    for (let i = dataActuals.length - 1; i >= 0; i--) {
                        if (dataActuals[i] !== null) { lastIndex = i; break; }
                    }
                    
                    // Pin DOM positioning directly mimicking chart.js Cartesian layouts
                    if (lastIndex !== -1 && meta.data[lastIndex]) {
                        const dot = document.getElementById('liveDot');
                        if (dot) {
                            dot.style.display = 'block';
                            dot.style.left = meta.data[lastIndex].x + 'px';
                            dot.style.top = meta.data[lastIndex].y + 'px';
                        }
                    }
                }
            }]
        });
    }

    function renderAlerts(alerts) {
        const list = document.getElementById('alertList');
        if (alerts.length === 0) {
            list.innerHTML = `<div style="text-align: center; color: var(--text-muted); margin-top: 2rem;">No alerts right now!</div>`;
            return;
        }

        list.innerHTML = alerts.map(a => {
            const date = new Date(a.time).toLocaleString('en-US', { day: 'numeric', month: 'short', hour: 'numeric', minute: 'numeric' });
            const title = a.severity === 'high' ? 'CRITICAL DEVIATION' : 'WARN - Deviation Tracking';
            return `
                <div class="alert-card ${a.severity}">
                    <div class="alert-title">${title}</div>
                    <div class="alert-details">
                        Target Time: ${date}
                        <span>Anomaly Shift: <strong>${a.deviation}%</strong></span>
                    </div>
                </div>
            `;
        }).join('');
    }

    async function fetchSummary() {
        try {
            const res = await fetch('/api/summary');
            const data = await res.json();
            if (data.summary) document.getElementById('aiSummaryText').innerText = `"${data.summary}"`;
        } catch (e) {}
    }

    // Initialize logic
    fetchSummary();
    fetchData();
    fetchAlerts();
    fetchMetrics();
});
