<template>
  <v-container
    fluid
  >
    <v-row>
      <v-col cols="12">
        <v-card shaped outlined elevation="2">
          <job-charts
            v-model="filter"
          />
        </v-card>
      </v-col>

      <v-col cols="12">
        <v-card shaped outlined elevation="2">
          <v-card-title>
            Job History
            <v-btn text rounded icon
                   @click="refresh"
            >
              <v-icon>refresh</v-icon>
            </v-btn>
          </v-card-title>
          <v-data-table
            :headers="headers"
            :items="jobs"
            :loading="loading"
            :options.sync="options"
            :server-items-length="total"
            :expanded.sync="expanded"
            show-expand
            expand-icon="expand_more"
            item-key="id"
            @click:row="clickRow"
            :footer-props="{
              itemsPerPageOptions: [10, 25, 50, 100, -1],
              prevIcon: 'navigate_before',
              nextIcon: 'navigate_next',
              firstIcon: 'first_page',
              lastIcon: 'last_page',
              showFirstLastPage: true
            }"
          >
            <template v-slot:item.args="{ item }">
              <v-chip
                color="#aabbcc"
                v-for="p in Object.entries(item.args) "
                :key="p[0]"
              >
                {{ p[0] }} : {{ p[1] }}
              </v-chip>
            </template>
            <template v-slot:item.phase="{ item }">
              <phase :phase="item.phase"/>
            </template>
            <template v-slot:item.status="{ item }">
              <status :status="item.status"/>
            </template>
            <template v-slot:expanded-item="{ item,headers }">
              <td :colspan="headers.length">
                <job-details :job="item.id"/>
              </td>
            </template>
          </v-data-table>
        </v-card>
      </v-col>
    </v-row>
  </v-container>
</template>

<script>
  import JobDetails from "@/components/JobDetails";
  import JobCharts from "@/components/JobCharts";
  import Filter from "@/mixins/Filter.js";
  import Status from '@/components/Status.vue'
  import Phase from '@/components/Phase.vue'
  import moment from "moment"

  export default {
    name: "JobHistory",
    mixins: [Filter],
    components: {JobCharts, JobDetails,Phase,Status},

    data() {
      return {
        jobs: [],
        expanded: [],
        options: {},
        total: 0,
        loading: false,
        headers: [
          { text: 'Job Run ID', value: 'id' },
          { text: 'Project Name', value: 'project' },
          { text: 'Project Version', value: 'version' },
          { text: 'Job Name', value: 'job' },
          { text: 'Parameters', value: 'args' },
          { text: 'Build Phase', value: 'phase' },
          { text: 'Status', value: 'status', width:120 },
          { text: 'Started at', value: 'startDateTime' },
          { text: 'Finished at', value: 'endDateTime' },
          { text: 'Duration', value: 'duration' },
          { text: 'Error message', value: 'error', width:160 },
        ]
      }
    },

    watch: {
      options: {
        handler () {
          this.getData()
        },
        deep: true,
      },
    },

    methods: {
      clickRow(item, event) {
        if(event.isExpanded) {
          const index = this.expanded.findIndex(i => i === item);
          this.expanded.splice(index, 1)
        } else {
          this.expanded.push(item);
        }
      },

      refresh() {
        this.options.page = 1
        this.getData()
      },

      getData() {
        const { page, itemsPerPage } = this.options
        const offset = (page-1)*itemsPerPage

        this.loading = true
        this.$api.getJobsHistory(this.filter.projects, this.filter.jobs, this.filter.phases, this.filter.status, offset, itemsPerPage)
          .then(response => {
            let dateFormat = 'MMM D, YYYY HH:mm:ss'
            response.data.forEach(item => {
              let start = moment(item.startDateTime)
              let end = moment(item.endDateTime)
              item.startDateTime = start.format(dateFormat)
              item.endDateTime = end.format(dateFormat)
              item.duration = moment.duration(end.diff(start)).humanize()
            })
            this.title = "Job History"
            this.jobs = response.data
            this.total = response.total
            this.expanded = []
            this.loading = false
          })
      }
    }
  }
</script>

<style>

</style>
