<template>
  <v-container fluid>
    <v-select
      v-model="selection"
      :items="status"
      chips
      label="Filter by Execution Status"
      multiple
      clearable
      deletable-chips
    ></v-select>
  </v-container>
</template>

<script>

export default {
  name: 'StatusSelector',

  props : {
    selection: [],
  },

  data() {
    return {
      status: ["SUCCESS", "SKIPPED", "FAILED"],
    };
  },

  mounted() {
  },
};
</script>

<style>
</style>
