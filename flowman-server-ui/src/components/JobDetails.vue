<template>
  <v-container fluid>
    <v-card-title>
      <v-icon>gavel</v-icon>
      Job '{{properties.project}}/{{properties.name}}' {{ properties.phase }} id {{job}} status {{properties.status}}
    </v-card-title>

    <v-container fluid>
      <h3>Targets</h3>
      <!--
      <v-timeline
        align-top
        dense
      >
        <v-timeline-item
          v-for="item in Array.from(targets).reverse()"
          :key="item.id"
          right
          small
        >
          <v-row>
            <v-col cols="2">
              {{ item.target }}
            </v-col>
            <v-col cols="3">
              <v-chip small
                      v-for="p in Object.entries(item.partitions) "
                      :key="p"
              >
                {{ p[0] }} : {{ p[1] }}
              </v-chip>
            </v-col>
            <v-col cols="1"><status :status="item.status"/></v-col>
            <v-col cols="1">{{ item.startDateTime }}</v-col>
            <v-col cols="1">{{ item.endDateTime }}</v-col>
            <v-col cols="4">{{ item.error }}</v-col>
          </v-row>
        </v-timeline-item>
      </v-timeline>
      -->

      <v-simple-table dense>
        <template v-slot:default>
          <thead>
          <tr>
            <th class="text-left">
              Run ID
            </th>
            <th class="text-left">
              Name
            </th>
            <th class="text-left">
              Partition
            </th>
            <th class="text-left">
              Status
            </th>
            <th class="text-left">
              Start
            </th>
            <th class="text-left">
              End
            </th>
            <th class="text-left">
              Duration
            </th>
            <th class="text-left">
              Error
            </th>
          </tr>
          </thead>
          <tbody>
          <tr
            v-for="item in Array.from(targets).reverse()"
            :key="item.id"
          >
            <td>{{ item.id }}</td>
            <td>{{ item.target }}</td>
            <td>
              <v-chip small
                color="#aacccc"
                v-for="p in Object.entries(item.partitions) "
                :key="p"
              >
                {{ p[0] }} : {{ p[1] }}
              </v-chip>
            </td>
            <td><status :status="item.status" small/></td>
            <td>{{ date(item.startDateTime) }}</td>
            <td>{{ date(item.endDateTime) }}</td>
            <td>{{ duration(item.duration) }}</td>
            <td>{{ item.error }}</td>
          </tr>
          </tbody>
        </template>
      </v-simple-table>
    </v-container>

    <v-container fluid
      v-if="environment.length > 0"
    >
      <h3>Environment</h3>
      <environment-table
        :environment = "environment"
      />
    </v-container>

    <v-container fluid>
      <h3>Metrics</h3>
      <metric-table
        :metrics = "metrics"
      />
    </v-container>
  </v-container>
</template>

<script>
import Status from '@/components/Status.vue'
import EnvironmentTable from '@/components/EnvironmentTable.vue'
import MetricTable from '@/components/MetricTable.vue'
import moment from "moment";

export default {
  name: 'JobDetails',
  components: {Status,EnvironmentTable,MetricTable},

  props: {
    job: String
  },

  data () {
    return {
      properties: {},
      metrics: [],
      targets: [],
      environment: []
    }
  },

  mounted() {
    this.refresh()
  },

  methods: {
    refresh() {
      this.$api.getJobDetails(this.job).then(response => {
        this.properties = {
          namespace: response.namespace,
          project: response.project,
          name: response.job,
          args: response.args,
          phase: response.phase,
          status: response.status,
          startDt: response.startDateTime,
          endDt: response.endDateTime,
          parameters: response.args,
          metrics: response.metrics
        }
        this.metrics = response.metrics
      })

      this.$api.getJobTargets(this.job).then(response => {
        this.targets = response.data
      })

      this.$api.getJobEnvironment(this.job).then(response => {
        this.environment = Object.entries(response.env)
      })
    },

    date(dt) {
      return moment(dt).format('MMM D, YYYY HH:mm:ss')
    },
    duration(dt) {
      return moment.duration(dt).humanize()
    }
  }
}
</script>
