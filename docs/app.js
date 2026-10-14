/* Shared helpers for the dashboard pages.
 *
 * Exposed as one global rather than ES modules: GitHub Pages serves these as
 * plain files with no build step, and a module graph would cost a second round
 * trip for no benefit at this size.
 */
(function (global) {
  "use strict";

  var css = function (name) {
    return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  };

  // One region, one colour, everywhere on both pages. Fixed by entity so a
  // chart with fewer regions never repaints the survivors.
  var REGIONS = ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"];
  var colourOf = function (region) {
    var i = REGIONS.indexOf(region);
    return css("--series-" + ((i < 0 ? 0 : i) + 1));
  };

  var num = function (value, digits) {
    if (value === null || value === undefined) return "—";
    return Number(value).toLocaleString("en-AU", {
      minimumFractionDigits: digits || 0, maximumFractionDigits: digits || 0
    });
  };

  // The sign goes before the currency symbol: "$-34k" reads as a typo, and
  // negative revenue is a real and important value in this market.
  var money = function (value) {
    if (value === null || value === undefined) return "—";
    var abs = Math.abs(value);
    var sign = value < 0 ? "-" : "";
    if (abs >= 1e6) return sign + "$" + (abs / 1e6).toFixed(2) + "m";
    if (abs >= 1e3) return sign + "$" + (abs / 1e3).toFixed(0) + "k";
    return sign + "$" + num(abs, 0);
  };

  var pct = function (value, digits) {
    if (value === null || value === undefined) return "—";
    return (value * 100).toFixed(digits === undefined ? 1 : digits) + "%";
  };

  // The CDN is a single point of failure for the charts but must not be one for
  // the page: without it the tiles, tables and quality report still carry the
  // whole story, so they render and the tables open by default.
  var charting = typeof Chart !== "undefined";
  if (charting) {
    Chart.defaults.font.family = getComputedStyle(document.body).fontFamily;
    Chart.defaults.color = css("--text-secondary");
    Chart.defaults.maintainAspectRatio = false;
    Chart.defaults.animation = false;
  }

  // Charts are rebuilt when a filter changes, so each canvas keeps its instance
  // and is destroyed before being drawn again. Without this Chart.js leaves the
  // old listeners attached and the tooltip reads from stale data.
  var live = {};

  function plot(id, config) {
    if (!charting) return;
    if (live[id]) { live[id].destroy(); }
    live[id] = new Chart(document.getElementById(id), config);
  }

  function chartsUnavailable() {
    Array.prototype.forEach.call(document.querySelectorAll(".chart"), function (box) {
      box.setAttribute("hidden", "");
    });
    Array.prototype.forEach.call(document.querySelectorAll(".toggle"), function (button) {
      var target = document.getElementById(button.dataset.target);
      if (target) { target.removeAttribute("hidden"); }
      button.setAttribute("hidden", "");
    });
    var banner = document.createElement("p");
    banner.className = "err";
    banner.textContent = "Charts need Chart.js from the CDN, which did not load. " +
      "The tables below carry the same numbers.";
    document.querySelector("header").appendChild(banner);
  }

  var axis = function (extra) {
    var base = {
      grid: { color: css("--grid"), drawTicks: false },
      border: { color: css("--axis") },
      ticks: { color: css("--text-muted"), padding: 6 }
    };
    return Object.assign(base, extra || {});
  };

  var legendBlock = {
    display: true,
    position: "bottom",
    labels: { boxWidth: 10, boxHeight: 10, usePointStyle: true, pointStyle: "circle", padding: 14 }
  };

  var escapeHtml = function (value) {
    return String(value === null || value === undefined ? "" : value)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  };

  function table(host, columns, rows) {
    var head = "<tr>" + columns.map(function (c) {
      return "<th" + (c.num ? ' class="num"' : "") + ">" + c.label + "</th>";
    }).join("") + "</tr>";
    var body = rows.map(function (row) {
      return "<tr>" + columns.map(function (c) {
        return "<td" + (c.num ? ' class="num"' : "") + ">" + c.get(row) + "</td>";
      }).join("") + "</tr>";
    }).join("");
    host.innerHTML = "<table><thead>" + head + "</thead><tbody>" + body + "</tbody></table>";
  }

  // The light-mode palette has three slots below 3:1 against the surface, so a
  // table view is required rather than optional. Every chart has one.
  function wireToggles() {
    Array.prototype.forEach.call(document.querySelectorAll(".toggle"), function (button) {
      button.addEventListener("click", function () {
        var target = document.getElementById(button.dataset.target);
        var open = target.hasAttribute("hidden");
        if (open) { target.removeAttribute("hidden"); } else { target.setAttribute("hidden", ""); }
        button.setAttribute("aria-pressed", String(open));
        button.textContent = open ? "Hide table" : "Table";
      });
    });
  }

  var get = function (path) {
    return fetch("data/" + path + ".json", { cache: "no-cache" }).then(function (r) {
      if (!r.ok) throw new Error(path + ": HTTP " + r.status);
      return r.json();
    });
  };

  function fail(message) {
    var footer = document.getElementById("footer-meta");
    if (!footer) return;
    footer.className = "err";
    footer.textContent = "Could not load dashboard data: " + message;
  }

  global.WIND = {
    REGIONS: REGIONS, colourOf: colourOf, css: css,
    num: num, money: money, pct: pct, escapeHtml: escapeHtml,
    charting: charting, plot: plot, chartsUnavailable: chartsUnavailable,
    axis: axis, legendBlock: legendBlock, table: table,
    wireToggles: wireToggles, get: get, fail: fail
  };
})(window);
