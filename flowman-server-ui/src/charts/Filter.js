export default {
  name: 'Filter',

  props: {
    filter: {
      type: Object,
      default() {
        return {
          projects: [],
          jobs: [],
          targets: [],
          phases: [],
          status: []
        }
      }
    },
  },

  computed: {
    projectFilter() { return this.filter.projects },
    jobFilter() { return this.filter.jobs },
    targetFilter() { return this.filter.targets },
    phaseFilter() { return this.filter.phases },
    statusFilter() { return this.filter.status }
  },

  watch: {
    projectFilter: function () { this.getData() },
    jobFilter: function () { this.getData() },
    targetFilter: function () { this.getData() },
    phaseFilter: function () { this.getData() },
    statusFilter: function () { this.getData() },
  },

  mounted() {
    this.getData()
  },

  methods: {
    getData() {
    }
  }
};
