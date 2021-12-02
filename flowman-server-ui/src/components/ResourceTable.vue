<template>
  <v-simple-table dense dark height="160">
    <thead>
    <tr>
      <th class="text-left">
        Type
      </th>
      <th class="text-left">
        Name
      </th>
      <th class="text-left">
        Partition
      </th>
    </tr>
    </thead>
    <tbody>
    <tr
      v-for="item in resources"
      :key="item.name"
    >
      <td>{{ item.category }}</td>
      <td>{{ item.name }}</td>
      <td>
        <v-chip small
                v-for="p in Object.entries(item.partition) "
                :key="p"
        >
          {{ p[0] }} : {{ p[1] }}
        </v-chip>
      </td>
    </tr>
    </tbody>
  </v-simple-table>
</template>

<script>
export default {
  name: 'ResourceTable',

  props: {
    resources: []
  },
}
</script>
