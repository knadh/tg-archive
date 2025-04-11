{% extends "base.html" %}

{% block content %}
<section>
    <h1>Channel Statistics</h1>
    
    <div class="stat-cards">
        <div class="stat-card">
            <div class="stat-title">Total Messages</div>
            <div class="stat-value">{{ total_messages }}</div>
        </div>
        
        <div class="stat-card">
            <div class="stat-title">Unique Users</div>
            <div class="stat-value">{{ total_users }}</div>
        </div>
        
        <div class="stat-card">
            <div class="stat-title">Photos</div>
            <div class="stat-value">{{ media_stats.photos }}</div>
        </div>
        
        <div class="stat-card">
            <div class="stat-title">Videos</div>
            <div class="stat-value">{{ media_stats.videos }}</div>
        </div>
    </div>
    
    <div class="chart-container">
        <h2>Messages Over Time</h2>
        <canvas id="monthlyChart"></canvas>
    </div>
    
    <div class="chart-container">
        <h2>Media Types</h2>
        <canvas id="mediaChart"></canvas>
    </div>
    
    <div class="top-users">
        <h2>Most Active Users</h2>
        <table class="user-table">
            <thead>
                <tr>
                    <th>User</th>
                    <th>Messages</th>
                </tr>
            </thead>
            <tbody>
                {% for item in top_users %}
                <tr>
                    <td>
                        <div class="user-info">
                            {% if item.user.avatar %}
                            <img src="./{{ config.media_dir }}/{{ item.user.avatar }}" alt="{{ item.user.username }}" class="avatar" style="width: 30px; height: 30px;">
                            {% endif %}
                            <span>@{{ item.user.username }}</span>
                        </div>
                    </td>
                    <td>{{ item.message_count }}</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>
</section>

<script src="./js/chart.min.js"></script>
<script>
document.addEventListener('DOMContentLoaded', function() {
    // Monthly messages chart
    const monthlyData = {{ monthly_counts|tojson }};
    const labels = monthlyData.map(item => item.date);
    const counts = monthlyData.map(item => item.count);
    
    const monthlyCtx = document.getElementById('monthlyChart').getContext('2d');
    new Chart(monthlyCtx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'Messages',
                data: counts,
                backgroundColor: '#1e88e5',
                borderColor: '#1e88e5',
                tension: 0.1
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: {
                    position: 'top',
                },
                title: {
                    display: true,
                    text: 'Messages Per Month'
                }
            }
        }
    });
    
    // Media types chart
    const mediaStats = {{ media_stats|tojson }};
    const mediaTypes = Object.keys(mediaStats);
    const mediaCounts = Object.values(mediaStats);
    
    const mediaCtx = document.getElementById('mediaChart').getContext('2d');
    new Chart(mediaCtx, {
        type: 'pie',
        data: {
            labels: mediaTypes,
            datasets: [{
                data: mediaCounts,
                backgroundColor: [
                    '#1e88e5', '#42a5f5', '#90caf9', '#bbdefb', 
                    '#64b5f6', '#2196f3', '#0d47a1'
                ]
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: {
                    position: 'top',
                },
                title: {
                    display: true,
                    text: 'Media Types Distribution'
                }
            }
        }
    });
});
</script>
{% endblock %}
