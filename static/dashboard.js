let charts = {};
let map;
let driverMarkers = [];

// Add Dark Mode Toggle
document.getElementById("dark-toggle").addEventListener("click", () => {
  document.body.classList.toggle("dark");
});

// Chart Configuration
function initCharts() {
  charts.throughput = new Chart(document.getElementById("chart-throughput"), {
    type: "line",
    data: { labels: [], datasets: [{ label: "Throughput", data: [], borderColor: "#3b82f6", borderWidth: 2 }] }
  });

  charts.latency = new Chart(document.getElementById("chart-latency"), {
    type: "line",
    data: { labels: [], datasets: [{ label: "Latency (ms)", data: [], borderColor: "#8b5cf6", borderWidth: 2 }] }
  });

  charts.retries = new Chart(document.getElementById("chart-retries"), {
    type: "bar",
    data: { labels: [], datasets: [{ label: "Retries", data: [], backgroundColor: "#f97316" }] }
  });

  charts.consistencyDelay = new Chart(document.getElementById("chart-consistency-delay"), {
    type: "line",
    data: { labels: [], datasets: [{ label: "Consistency Delay", data: [], borderColor: "#10b981", borderWidth: 2 }] }
  });

  charts.driverStatus = new Chart(document.getElementById("chart-driver-status"), {
    type: "pie",
    data: { labels: [], datasets: [{ data: [], backgroundColor: ["#2563eb", "#10b981", "#f59e0b"] }] }
  });

  charts.rideStates = new Chart(document.getElementById("chart-ride-states"), {
    type: "bar",
    data: {
      labels: ["Requested", "Assigned", "En Route", "Completed"],
      datasets: [{ data: [0, 0, 0, 0], backgroundColor: ["#F29339", "#2563eb", "#10b981", "#6b7280"] }]
    }
  });
}

// Update data
function updateChart(chart, value) {
  const label = new Date().toLocaleTimeString();
  chart.data.labels.push(label);
  chart.data.datasets[0].data.push(value);
  if (chart.data.labels.length > 20) chart.data.labels.shift(), chart.data.datasets[0].data.shift();
  chart.update();
}

// Map
function initMap() {
  map = L.map("driver-map").setView([40.75, -73.97], 11);
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", { maxZoom: 18 }).addTo(map);
}

async function refreshDashboard() {
  const metrics = await (await fetch("/api/metrics")).json();
  const drivers = await (await fetch("/api/drivers")).json();
  const rides = await (await fetch("/api/rides")).json();

  // Metrics
  document.getElementById("m-total-rides").textContent = metrics.total_rides || 0;
  document.getElementById("m-completed").textContent = metrics.completed_trips || 0;
  document.getElementById("m-avg-distance").textContent = metrics.avg_distance?.toFixed(2) || "–";
  document.getElementById("m-avg-amount").textContent = metrics.avg_amount?.toFixed(2) || "–";
  document.getElementById("m-completion-rate").textContent = metrics.total_rides ? ((metrics.completed_trips / metrics.total_rides) * 100).toFixed(1) + "%" : "0%";

  // Update charts
  updateChart(charts.throughput, metrics.throughput || 0);
  updateChart(charts.latency, metrics.avg_latency_ms || 0);
  updateChart(charts.consistencyDelay, metrics.consistency_delay_ms || 0);
  updateChart(charts.retries, metrics.transaction_retries || 0);

  // Driver status
  if (metrics.drivers_by_status) {
    charts.driverStatus.data.labels = Object.keys(metrics.drivers_by_status);
    charts.driverStatus.data.datasets[0].data = Object.values(metrics.drivers_by_status);
    charts.driverStatus.update();
  }

  // Driver Map
  driverMarkers.forEach((m) => map.removeLayer(m));
  driverMarkers = [];
  drivers.forEach((d) => {
    if (d.lat && d.lon) {
      const marker = L.circleMarker([d.lat, d.lon], { radius: 5, color: "#3b82f6" }).addTo(map);
      marker.bindPopup(`<b>${d.name}</b><br>Status: ${d.status}`);
      driverMarkers.push(marker);
    }
  });

  // Tables
  document.getElementById("drivers-table-body").innerHTML = drivers
    .map(d => `<tr><td>${d.name}</td><td>${d.status}</td><td>${d.lon?.toFixed(4)}</td><td>${d.lat?.toFixed(4)}</td><td>${d.last_updated}</td></tr>`).join("");

  document.getElementById("rides-table-body").innerHTML = rides
    .map(r => `<tr><td>${r.requested_at}</td><td>${r.assigned_driver}</td><td>${r.status}</td><td>${r.pickup_lat?.toFixed(4)}, ${r.pickup_lon?.toFixed(4)}</td><td>${r.dropoff_lat?.toFixed(4)}, ${r.dropoff_lon?.toFixed(4)}</td></tr>`).join("");
}

initCharts();
initMap();
refreshDashboard();
setInterval(refreshDashboard, 3000);
