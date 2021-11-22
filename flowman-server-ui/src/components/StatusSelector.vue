<template>
  <v-container fluid>
    <v-row align="center">
      <v-col cols="11">
        <v-select
          v-model="value"
          :items="status"
          chips
          outlined
          label="Filter by Execution Status"
          multiple
          clearable
          deletable-chips
          append-icon="expand_more"
          clear-icon="clear"
          @input='$emit("input", value)'
        ></v-select>
      </v-col>
    </v-row>
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
