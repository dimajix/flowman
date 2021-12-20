<template>
  <v-card rounded shaped>
    <screen id="screen" ref="screen" :markers="[]" height="600">
    </screen>
  </v-card>
</template>


<script>
import Screen from 'vnodes/src/components/Screen'

export default {
  name: 'Flow',
  components: {Screen},
}
</script>
