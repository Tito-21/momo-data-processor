// AMAzing SMS - Chart Handler
// Handles data visualization and chart rendering

class ChartHandler {
    constructor() {
        this.charts = {};
        this.data = {
            totalMessages: 0,
            processedToday: 0,
            successRate: 0,
            errorCount: 0,
            categories: {},
            timeline: []
        };
        
        this.init();
    }
    
    init() {
        this.loadData();
        this.updateStats();
        this.renderCharts();
    }
    
    async loadData() {
        try {
            // This will be connected to the API later
            // For now, we'll use mock data
            this.data = {
                totalMessages: 1250,
                processedToday: 45,
                successRate: 98.5,
                errorCount: 2,
                categories: {
                    'Payment': 35,
                    'Transfer': 28,
                    'Balance': 15,
                    'Other': 8
                },
                timeline: [
                    { time: '00:00', count: 5 },
                    { time: '04:00', count: 3 },
                    { time: '08:00', count: 12 },
                    { time: '12:00', count: 18 },
                    { time: '16:00', count: 15 },
                    { time: '20:00', count: 8 }
                ]
            };
        } catch (error) {
            console.error('Error loading data:', error);
        }
    }
    
    updateStats() {
        document.getElementById('total-messages').textContent = this.data.totalMessages.toLocaleString();
        document.getElementById('processed-today').textContent = this.data.processedToday.toLocaleString();
        document.getElementById('success-rate').textContent = this.data.successRate + '%';
        document.getElementById('error-count').textContent = this.data.errorCount.toLocaleString();
    }
    
    renderCharts() {
        this.renderCategoryChart();
        this.renderTimelineChart();
    }
    
    renderCategoryChart() {
        const ctx = document.getElementById('category-chart').getContext('2d');
        
        const labels = Object.keys(this.data.categories);
        const values = Object.values(this.data.categories);
        const colors = [
            '#FF6384',
            '#36A2EB',
            '#FFCE56',
            '#4BC0C0',
            '#9966FF',
            '#FF9F40'
        ];
        
        this.charts.category = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: labels,
                datasets: [{
                    data: values,
                    backgroundColor: colors.slice(0, labels.length),
                    borderWidth: 2,
                    borderColor: '#fff'
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: {
                            padding: 20,
                            usePointStyle: true
                        }
                    }
                }
            }
        });
    }
    
    renderTimelineChart() {
        const ctx = document.getElementById('timeline-chart').getContext('2d');
        
        const labels = this.data.timeline.map(item => item.time);
        const values = this.data.timeline.map(item => item.count);
        
        this.charts.timeline = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Messages Processed',
                    data: values,
                    borderColor: '#36A2EB',
                    backgroundColor: 'rgba(54, 162, 235, 0.1)',
                    borderWidth: 3,
                    fill: true,
                    tension: 0.4,
                    pointBackgroundColor: '#36A2EB',
                    pointBorderColor: '#fff',
                    pointBorderWidth: 2,
                    pointRadius: 6
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        beginAtZero: true,
                        grid: {
                            color: 'rgba(0,0,0,0.1)'
                        }
                    },
                    x: {
                        grid: {
                            color: 'rgba(0,0,0,0.1)'
                        }
                    }
                },
                plugins: {
                    legend: {
                        display: false
                    }
                }
            }
        });
    }
    
    refresh() {
        this.loadData().then(() => {
            this.updateStats();
            this.renderCharts();
        });
    }
}

// Initialize the chart handler when the page loads
document.addEventListener('DOMContentLoaded', () => {
    // Note: Chart.js will need to be loaded for this to work
    // For now, this is a placeholder structure
    console.log('Chart handler initialized');
    
    // Mock initialization for development
    const chartHandler = new ChartHandler();
    
    // Refresh data every 30 seconds
    setInterval(() => {
        chartHandler.refresh();
    }, 30000);
});
