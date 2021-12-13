import { Doughnut, mixins } from "vue-chartjs";

export default {
  extends: Doughnut,
  mixins: [ mixins.reactiveProp ],

  props: {
    titlePosition: {
      type: String,
      default: () => 'top'
    }
  },

  mounted() {
    // this.chartData is created in the mixin.
    // If you want to pass options please create a local options object
    let options = {
      responsive: true,
      title: {
        display: true,
        position: this.titlePosition,
        text: this.chartData.title,
        fontSize: 24
      },
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
