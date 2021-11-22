import { Doughnut, mixins } from "vue-chartjs";

export default {
  extends: Doughnut,
  mixins: [ mixins.reactiveProp ],

  mounted() {
    // this.chartData is created in the mixin.
    // If you want to pass options please create a local options object
    let options = {
      responsive: true,
        maintainAspectRatio: true,
        hoverBorderWidth: 20,
        legend: {
          position: 'right',
          align: 'center'
        },
      }
    this.renderChart(this.chartData, options);
  }
};
