<template>
  <v-container fluid>
    <v-card-title>
      <v-icon>gavel</v-icon>
      Job '{{properties.project}}/{{properties.name}}' {{ properties.phase }} {{job}} status {{properties.status}}
    </v-card-title>

    <h3>Targets</h3>
    <v-simple-table>
      <template v-slot:default>
        <thead>
        <tr>
          <th class="text-left">
            Name
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
        </tr>
        </thead>
        <tbody>
        <tr
          v-for="item in targets"
          :key="item.id"
        >
          <td>{{ item.target }}</td>
          <td><v-icon>{{ getIcon(item.status) }}</v-icon> {{ item.status }}</td>
          <td>{{ item.startDateTime }}</td>
          <td>{{ item.endDateTime }}</td>
        </tr>
        </tbody>
      </template>
    </v-simple-table>

    <h3>Metrics</h3>
    <v-simple-table>
      <template v-slot:default>
        <thead>
        <tr>
          <th class="text-left">
            Name
          </th>
          <th class="text-left">
            Labels
          </th>
          <th class="text-left">
            Value
          </th>
        </tr>
        </thead>
        <tbody>
        <tr
          v-for="item in metrics"
          :key="item.name"
        >
          <td>{{ item.name }}</td>
          <td>
            <v-chip
              v-for="p in Object.entries(item.labels) "
              :key="p"
            >
              {{ p[0] }} : {{ p[1] }}
            </v-chip>
          </td>
          <td>{{ item.value }}</td>
        </tr>
        </tbody>
      </template>
    </v-simple-table>
  </v-container>
</template>

<script>
export default {
  name: 'JobHistoryDetails',

  props: {
    job: String
  },

  data () {
    return {
      properties: {},
      metrics: [],
      targets: []
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
    },

    getIcon(status) {
      if (status === "SUCCESS") {
        return "done_all"
      }
      else if (status === "SKIPPED") {
        return "fast_forward"
      }
      else if (status === "FAILED") {
        return "error"
      }
      else {
        return "warning_amber"
      }
    }
  },

}
</script>
