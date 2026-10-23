// Punto de entrada de la app (layouts/app_shell.html, <script type="module">).
// Cada módulo registra sus listeners en init(); el orden de los enhancers de contenido
// (barcode, home-charts, notes) es el mismo que tenía el antiguo app.js.
import { init as initContent } from './core/content.js';
import { init as initActivity } from './core/activity.js';
import { init as initTabs } from './core/tabs.js';
import { init as initModals } from './core/modals.js';
import { init as initSwap } from './core/swap.js';
import { init as initBarcode } from './components/barcode.js';
import { init as initHomeCharts } from './components/home-charts.js';
import { init as initNotes } from './components/notes.js';
import { init as initWidgetChart } from './components/widget-chart.js';
import { init as initProductSuggest } from './components/product-suggest.js';
import { init as initFormHelpers } from './components/form-helpers.js';
import { init as initTransfers } from './features/transfers.js';
import { init as initTheme } from './features/theme.js';
import { init as initAgentChat } from './features/agent-chat.js';

initContent();
initActivity();
initTabs();
initModals();
initSwap();
initBarcode();
initHomeCharts();
initNotes();
initWidgetChart();
initProductSuggest();
initFormHelpers();
initTransfers();
initTheme();
initAgentChat();
