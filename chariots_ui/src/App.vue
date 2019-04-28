<template>
  <v-app id="inspire" dark>
    <v-navigation-drawer
      temporary  
      v-model="drawer"
      clipped
      fixed
      app
    >
      <v-list dense>
        <v-list-tile  v-for="series in this.seriesName" @click="pushRoute('/' +series)">
          <v-list-tile-action>
            <v-icon >trending_up</v-icon>
          </v-list-tile-action>
          <v-list-tile-content>
            <v-list-tile-title>{{series}}</v-list-tile-title>
          </v-list-tile-content>
        </v-list-tile>
      </v-list>
    </v-navigation-drawer>
    <v-toolbar app fixed clipped-left>
      <v-toolbar-side-icon @click.stop="drawer = !drawer"></v-toolbar-side-icon>
      <v-toolbar-title>Chariots Ui</v-toolbar-title>
      <v-spacer></v-spacer>
      <v-toolbar-items class="hidden-sm-and-down">
        <v-btn flat @click.stop="fetchData"><v-icon>cached</v-icon></v-btn>
      </v-toolbar-items>
    </v-toolbar>
    <v-content>
      <v-container fluid fill-height>
         <router-view></router-view>
      </v-container>
    </v-content>
    <v-footer app fixed>
      <span>&copy; 2017</span>
    </v-footer>
  </v-app>
</template>

<script>
import HelloWorld from './components/HelloWorld';
import {mapGetters, mapActions} from 'vuex';

export default {
  name: 'App',
  components: {
    HelloWorld
  },
  computed:{
    ...mapGetters([
      "seriesName",
    ])
  },
  methods: {
    ...mapActions(["fetchData"]),
    pushRoute(route) {
      this.$router.push(route)
    }
  },
  data: () => ({
      drawer: null
    }),
    props: {
      source: String
    }
}
</script>
