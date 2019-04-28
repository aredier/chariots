<template>
  <div>
    <h1>This is not yet a data page for {{this.series_name}}</h1>
  </div>
</template>
<script>
export default {
  data: () => ({
    series_name: "NA"
  }),
  watch: {
    '$route' (to, from) {
      this.series_name = to.params.series_name
    }
  }
}
</script>

