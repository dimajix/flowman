<template>
  <v-simple-table dense>
    <template v-slot:default>
      <thead>
      <tr>
        <th class="text-left">
          Name
        </th>
        <th class="text-left">
          Value
        </th>
      </tr>
      </thead>
      <tbody>
      <tr
        v-for="item in environment.sort((l,r) => l[0] >= r[0])"
        :key="item[0]"
      >
        <td>{{ item[0] }}</td>
        <td>{{ item[1] }}</td>
      </tr>
      </tbody>
    </template>
  </v-simple-table>
</template>


<script>
export default {
  name: 'EnvironmentTable',

  props: {
    environment: Array[String]
  },
}
</script>
