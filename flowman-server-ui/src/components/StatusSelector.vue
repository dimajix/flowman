<template>
  <v-container fluid>
    <v-select
      v-model="value"
      :items="status"
      chips
      label="Filter by Execution Status"
      multiple
      clearable
      deletable-chips
      @input='$emit("input", value)'
    ></v-select>
  </v-container>
</template>

<script>

export default {
  name: 'StatusSelector',

  data() {
    return {
      value: [],
      status: ["SUCCESS", "SKIPPED", "FAILED"],
    };
  },

  mounted() {
  },
};
</script>

<style>
</style>
