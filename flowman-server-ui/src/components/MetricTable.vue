<template>
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
        v-for="item in sortMetrics(metrics).entries()"
        :key="hash(item[0])"
      >
        <td>{{ item[1].name }}</td>
        <td>
          <v-chip
            v-for="p in Object.entries(item[1].labels) "
            :key="hash(p)"
          >
            {{ p[0] }} : {{ p[1] }}
          </v-chip>
        </td>
        <td>{{ item[1].value }}</td>
      </tr>
      </tbody>
    </template>
  </v-simple-table>
</template>

<script>
let hash = require('object-hash');

export default {
  name: 'MetricTable',

  props: {
    metrics: Array[String]
  },

  methods: {
    sortMetrics(items) {
      let items2 = items.slice()
      items2.sort((a,b) => a.name.localeCompare(b.name))
      return items2
    },
    hash(obj) {
      return hash(obj)
    }
  }
}
</script>
