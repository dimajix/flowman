<template>
  <v-container fluid>
    <v-row align="center">
      <v-col cols="2">
        <v-subheader>Status</v-subheader>
      </v-col>
      <v-col cols="9">
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
