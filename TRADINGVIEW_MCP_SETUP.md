# TradingView MCP Setup Guide

Quick start to connect Claude Code with TradingView Desktop.

## Prerequisites

- **TradingView Desktop** (Windows, macOS, or Linux)
- **Node.js 18+**
- **Claude Code** (web or desktop)

## 1. Start TradingView with Remote Debugging

### macOS
```bash
/Applications/TradingView.app/Contents/MacOS/TradingView --remote-debugging-port=9222
```

### Windows (PowerShell)
```powershell
& "C:\Program Files\TradingView\TradingView.exe" --remote-debugging-port=9222
```

### Linux
```bash
tradingview --remote-debugging-port=9222
```

**Verify:** Open http://localhost:9222/json/list in a browser. You should see a JSON list with TradingView targets.

## 2. Install MCP Server

```bash
cd tradingview-mcp
npm install
npm start
```

You should see:
```
⚠  tradingview-mcp  |  Unofficial tool. Not affiliated with TradingView Inc. or Anthropic.
   Ensure your usage complies with TradingView's Terms of Use.
```

This is **stdio mode** — it's listening on stdin/stdout, ready to receive MCP calls.

## 3. Configure Claude Code

In your Claude Code settings (`.claude/settings.json`), add:

```json
{
  "mcpServers": {
    "tradingview": {
      "command": "node",
      "args": ["/path/to/tradingview-mcp/src/server.js"]
    }
  }
}
```

Replace `/path/to` with the actual path to your tradingview-mcp directory.

## 4. Verify Connection

In Claude Code, ask:

```
Use the `chart_get_state` tool to read the current chart state.
```

Expected response: Symbol, timeframe, chart type, and list of indicators.

---

## Usage Examples

### Read Chart State
```javascript
// Get current symbol, timeframe, indicators
chart_get_state
```

### Change Symbol
```javascript
chart_set_symbol
  symbol: "BTCUSD"
```

### Get Real-Time Quote
```javascript
quote_get
// Returns: {last, open, high, low, close, volume}
```

### Read Indicator Values
```javascript
data_get_study_values
  summary: true
// Returns: {RSI: 65, MACD: {...}, BB: {...}}
```

### Read OHLCV Data
```javascript
data_get_ohlcv
  count: 20
  summary: true
// Returns: [{time, open, high, low, close, volume}, ...]
```

### Add an Indicator
```javascript
chart_manage_indicator
  action: "add"
  indicator: "Relative Strength Index"
  inputs: '{"length": 14}'
```

### Draw a Trend Line
```javascript
draw_shape
  shape: "trend_line"
  point: {time: 1700000000, price: 50000}
  point2: {time: 1700100000, price: 51000}
```

### Take a Screenshot
```javascript
capture_screenshot
  region: "chart"
```

### Write & Compile Pine Script
```javascript
pine_set_source
  source: "//@version=6\nindicator(\"MyIndicator\")..."

pine_smart_compile
// Returns compile errors (if any)
```

---

## 78 Tools Available

| Category | Tools | Examples |
|----------|-------|----------|
| **Chart** | 8 | get_state, set_symbol, set_timeframe, set_type |
| **Data** | 12 | get_study_values, get_ohlcv, get_pine_lines, get_pine_labels |
| **Pine** | 7 | set_source, smart_compile, get_errors, get_console |
| **Drawing** | 5 | draw_shape, get_shapes, delete_shape |
| **Replay** | 5 | start, step, trade, status, stop |
| **Capture** | 3 | screenshot, chart_image, strategy_tester_image |
| **Alerts** | 3 | create, list, delete |
| **Panes** | 4 | list, set_layout, focus, set_symbol |
| **Tabs** | 3 | list, new, close, switch |
| **Indicators** | 3 | set_inputs, get_inputs, describe |
| **Batch** | 3 | run_across_symbols, run_across_timeframes |
| **UI** | 1 | show_message |
| **Watchlist** | 2 | list, add |
| **Health** | 2 | ping, describe |

---

## Troubleshooting

### "No TradingView chart target found"
- Check TradingView is running with `--remote-debugging-port=9222`
- Verify port 9222 is open: `curl http://localhost:9222/json/list`
- Make sure you have a chart open in TradingView (not just the symbol search)

### "Cannot connect to TradingView"
- TradingView may have crashed. Restart it with the debugging port.
- Check if another process is using port 9222: `lsof -i :9222` (macOS/Linux)

### "Script returned no value"
- Some tools (like `chart_set_symbol`) don't return a value, only success/error.
- Check the `success: true` flag in the response.

### "Cannot read property 'value' of undefined"
- The TradingView API may not be ready. Try `chart_get_state` first to wait for initialization.
- Some tools require the chart to be fully loaded (watch stderr for errors).

### Port 9222 Already in Use
```bash
# Find what's using it (macOS/Linux)
lsof -i :9222

# Kill it if needed
kill -9 <PID>
```

---

## Best Practices

1. **Always start with `chart_get_state`** to initialize and get entity IDs
2. **Use `summary: true`** on data tools to reduce token usage
3. **Use `study_filter`** on Pine tools to target specific indicators
4. **Avoid `verbose: true`** unless you need raw data (can be 200KB+)
5. **Store entity IDs** from chart state for indicator operations
6. **Never expose port 9222** to the internet
7. **Keep TradingView updated** for security patches
8. **Review LLM prompts** — they execute arbitrary JS in TradingView context

---

## File Structure

```
tradingview-mcp/
├── src/
│   ├── server.js              # Main MCP server
│   ├── connection.js          # CDP protocol + sanitization
│   ├── cli/
│   │   └── index.js          # CLI tool (tv command)
│   ├── core/                 # Core logic (chart, data, pine, etc.)
│   └── tools/                # MCP tool registration
├── tests/
│   ├── sanitization.test.js  # Security tests
│   ├── pine_analyze.test.js  # Unit tests
│   └── e2e.test.js          # Integration tests
├── package.json
├── README.md
├── SECURITY.md
└── SETUP_GUIDE.md
```

---

## CLI Access

Every MCP tool is also available as a `tv` command:

```bash
# List available commands
node src/cli/index.js --help

# Read chart state
node src/cli/index.js chart:state

# Change symbol
node src/cli/index.js chart:symbol -s AAPL

# Stream live quote
node src/cli/index.js stream:quote
```

---

## Next: Integration with binance-collector

Once you've verified the MCP server works locally, you can integrate it with Claude Code's binance-collector project for:

- Real-time chart analysis for trading signals
- Pine Script development assisted by Claude
- Multi-symbol grid layout analysis
- Replay mode backtesting

See `TRADINGVIEW_MCP_SECURITY_ANALYSIS.md` for security considerations.

