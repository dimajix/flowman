<template>
  <v-container fluid>
    <v-card-title>
      <v-icon>gavel</v-icon>
      Job '{{properties.project}}/{{properties.name}}' {{ properties.phase }} id {{job}} status {{properties.status}}
    </v-card-title>

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
              v-for="p in Object.entries(item.partitions) "
              :key="p"
            >
              {{ p[0] }} : {{ p[1] }}
            </v-chip>
          </td>
          <td><status :status="item.status" small/></td>
          <td>{{ item.startDateTime }}</td>
          <td>{{ item.endDateTime }}</td>
          <td>{{ item.error }}</td>
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
import Status from '@/components/Status.vue'

export default {
  name: 'JobDetails',
  components: {Status},

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
    },
  }
}
</script>
